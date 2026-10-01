"""Validation and normalization of schema-less Debezium PostgreSQL records."""

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Protocol, cast

SUPPORTED_OPERATIONS = frozenset({"r", "c", "u", "d"})


class CdcRecordError(ValueError):
    """A Kafka record does not satisfy the raw CDC event contract."""


class KafkaRecord(Protocol):
    """Subset of confluent-kafka Message used by the parser and test fakes."""

    def topic(self) -> str: ...

    def partition(self) -> int: ...

    def offset(self) -> int: ...

    def key(self) -> bytes | None: ...

    def value(self) -> bytes | None: ...

    def timestamp(self) -> tuple[int, int] | None: ...


@dataclass(frozen=True)
class CdcEvent:
    """One validated Debezium envelope plus its Kafka transport identity."""

    event_id: str
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    kafka_timestamp: datetime | None
    source_schema: str
    source_table: str
    operation: str
    source_lsn: int | None
    source_tx_id: int | None
    source_timestamp: datetime | None
    key_json: str
    envelope_json: str
    before_json: str | None
    after_json: str | None
    event_date: date
    ingested_at: datetime


def event_identity(topic: str, partition: int, offset: int) -> str:
    """Return a stable, compact encoding of one Kafka transport coordinate."""
    coordinate = f"{topic}:{partition}:{offset}"
    return hashlib.sha256(coordinate.encode("utf-8")).hexdigest()


def _context(record: KafkaRecord) -> str:
    return f"topic={record.topic()} partition={record.partition()} offset={record.offset()}"


def _decode_json(raw: bytes | None, *, field: str, context: str) -> tuple[str, object]:
    if raw is None:
        raise CdcRecordError(f"{context}: broker tombstone ({field}=null) is not allowed")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CdcRecordError(f"{context}: {field} is not UTF-8") from exc
    try:
        return text, json.loads(text)
    except json.JSONDecodeError as exc:
        raise CdcRecordError(f"{context}: {field} is not valid JSON") from exc


def _optional_mapping_json(value: object, *, field: str, context: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise CdcRecordError(f"{context}: envelope.{field} must be an object or null")
    return json.dumps(value, separators=(",", ":"), sort_keys=True, ensure_ascii=False)


def _integer(value: object, *, field: str, context: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise CdcRecordError(f"{context}: source.{field} must be an integer or null")
    try:
        return int(cast(int | str, value))
    except (TypeError, ValueError) as exc:
        raise CdcRecordError(f"{context}: source.{field} must be an integer or null") from exc


def _epoch_moment(container: Mapping[str, object]) -> datetime | None:
    for field, divisor in (("ts_ns", 1_000_000_000), ("ts_us", 1_000_000), ("ts_ms", 1_000)):
        value = container.get(field)
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, int):
            return None
        return datetime.fromtimestamp(value / divisor, tz=UTC)
    return None


def _kafka_moment(record: KafkaRecord, context: str) -> datetime | None:
    stamp = record.timestamp()
    if stamp is None:
        return None
    _, milliseconds = stamp
    if milliseconds < 0:
        return None
    try:
        return datetime.fromtimestamp(milliseconds / 1_000, tz=UTC)
    except (OverflowError, OSError, ValueError) as exc:
        raise CdcRecordError(f"{context}: Kafka timestamp is outside the supported range") from exc


def parse_debezium_record(
    record: KafkaRecord,
    *,
    topic_tables: Mapping[str, str],
    ingested_at: datetime,
) -> CdcEvent:
    """Validate and normalize one schema-less Debezium PostgreSQL message.

    Raw key/envelope JSON text is retained exactly after UTF-8 decoding. Parsed
    fields are routing and indexing conveniences only.
    """
    context = _context(record)
    expected_table = topic_tables.get(record.topic())
    if expected_table is None:
        raise CdcRecordError(f"{context}: topic is not in the CDC allow-list")
    if record.partition() < 0 or record.offset() < 0:
        raise CdcRecordError(f"{context}: Kafka partition and offset must be non-negative")

    key_json, key = _decode_json(record.key(), field="key", context=context)
    envelope_json, envelope_value = _decode_json(record.value(), field="value", context=context)
    if not isinstance(key, Mapping) or not key:
        raise CdcRecordError(f"{context}: key must be a non-empty JSON object")
    if not isinstance(envelope_value, Mapping):
        raise CdcRecordError(f"{context}: envelope must be a JSON object")
    envelope = cast(Mapping[str, object], envelope_value)

    operation = envelope.get("op")
    if not isinstance(operation, str) or operation not in SUPPORTED_OPERATIONS:
        raise CdcRecordError(f"{context}: unsupported Debezium operation {operation!r}")
    source_value = envelope.get("source")
    if not isinstance(source_value, Mapping):
        raise CdcRecordError(f"{context}: envelope.source must be an object")
    source = cast(Mapping[str, object], source_value)
    source_schema = source.get("schema")
    source_table = source.get("table")
    if source_schema != "public" or source_table != expected_table:
        raise CdcRecordError(
            f"{context}: route mismatch, expected public.{expected_table}, "
            f"got {source_schema!r}.{source_table!r}"
        )

    before_json = _optional_mapping_json(envelope.get("before"), field="before", context=context)
    after_json = _optional_mapping_json(envelope.get("after"), field="after", context=context)
    if operation in {"r", "c", "u"} and after_json is None:
        raise CdcRecordError(f"{context}: operation {operation!r} requires envelope.after")
    if operation == "d" and (before_json is None or after_json is not None):
        raise CdcRecordError(f"{context}: delete requires before object and null after")

    source_timestamp = _epoch_moment(source)
    kafka_timestamp = _kafka_moment(record, context)
    event_moment = source_timestamp or kafka_timestamp
    if event_moment is None:
        raise CdcRecordError(f"{context}: neither source nor Kafka timestamp is available")
    normalized_ingested_at = (
        ingested_at.astimezone(UTC) if ingested_at.tzinfo else ingested_at.replace(tzinfo=UTC)
    )

    return CdcEvent(
        event_id=event_identity(record.topic(), record.partition(), record.offset()),
        kafka_topic=record.topic(),
        kafka_partition=record.partition(),
        kafka_offset=record.offset(),
        kafka_timestamp=kafka_timestamp,
        source_schema="public",
        source_table=expected_table,
        operation=operation,
        source_lsn=_integer(source.get("lsn"), field="lsn", context=context),
        source_tx_id=_integer(source.get("txId"), field="txId", context=context),
        source_timestamp=source_timestamp,
        key_json=key_json,
        envelope_json=envelope_json,
        before_json=before_json,
        after_json=after_json,
        event_date=event_moment.date(),
        ingested_at=normalized_ingested_at,
    )
