"""At-least-once Kafka consumer with an idempotent Iceberg sink."""

# pyright: reportMissingImports=false

import logging
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, cast

from confluent_kafka import TopicPartition

from omni_retail.ingestion.common.logging import context_logger
from omni_retail.lakehouse.trino import (
    TrinoExecutor,
    is_transient_catalog_error,
    is_transient_trino_error,
)

from .model import CdcEvent, KafkaRecord, parse_debezium_record
from .sql import ensure_cdc_table, merge_cdc_events_statement

logger = logging.getLogger(__name__)


class ConsumedRecord(KafkaRecord, Protocol):
    def error(self) -> object | None: ...


class KafkaConsumer(Protocol):
    def subscribe(self, topics: Sequence[str]) -> None: ...

    def consume(self, num_messages: int, timeout: float) -> Sequence[ConsumedRecord]: ...

    def commit(
        self, *, offsets: list[TopicPartition], asynchronous: bool
    ) -> list[TopicPartition] | None: ...

    def close(self) -> None: ...


class CdcTrinoExecutor(TrinoExecutor, Protocol):
    def execute_with_params(self, sql: str, params: list[object]) -> None: ...


class BatchWriter(Protocol):
    def ensure_table(self) -> None: ...

    def write(self, events: Sequence[CdcEvent]) -> None: ...


class CdcConsumerError(RuntimeError):
    """Kafka consumption or synchronous offset commit failed."""


class CdcBatchWriter:
    """Bounded-retry Trino writer for insert-only Iceberg MERGE batches."""

    def __init__(
        self,
        executor: CdcTrinoExecutor,
        *,
        catalog: str,
        schema: str,
        attempts: int = 3,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if attempts <= 0:
            raise ValueError("attempts must be greater than zero")
        self._executor = executor
        self._catalog = catalog
        self._schema = schema
        self._attempts = attempts
        self._sleep = sleep

    def ensure_table(self) -> None:
        ensure_cdc_table(self._executor, catalog=self._catalog, schema=self._schema)

    def write(self, events: Sequence[CdcEvent]) -> None:
        if not events:
            return
        statement, params = merge_cdc_events_statement(
            events, catalog=self._catalog, schema=self._schema
        )
        log = context_logger(
            __name__,
            event_count=len(events),
            first_event_id=events[0].event_id,
            last_event_id=events[-1].event_id,
        )
        for attempt in range(1, self._attempts + 1):
            try:
                self._executor.execute_with_params(statement, params)
                log.info("durable Iceberg CDC batch committed")
                return
            except Exception as exc:
                transient = is_transient_catalog_error(exc) or is_transient_trino_error(exc)
                if not transient or attempt == self._attempts:
                    raise
                delay = float(2 ** (attempt - 1))
                log.warning(
                    "transient Trino/Iceberg write failure; retrying attempt=%s/%s delay=%ss",
                    attempt,
                    self._attempts,
                    delay,
                )
                self._sleep(delay)


def _commit_offsets(records: Sequence[ConsumedRecord]) -> list[TopicPartition]:
    """Build Kafka's next-offset commits for every partition in a batch."""
    highest: dict[tuple[str, int], int] = {}
    for record in records:
        coordinate = (record.topic(), record.partition())
        highest[coordinate] = max(highest.get(coordinate, -1), record.offset())
    return [
        TopicPartition(topic, partition, offset + 1)
        for (topic, partition), offset in sorted(highest.items())
    ]


class CdcRunner:
    """Poll, validate, persist, then synchronously commit Kafka offsets."""

    def __init__(
        self,
        consumer: KafkaConsumer,
        writer: BatchWriter,
        *,
        topics: Sequence[str],
        topic_tables: Mapping[str, str],
        batch_size: int,
        poll_timeout_seconds: float,
        readiness_file: Path,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than zero")
        if poll_timeout_seconds <= 0:
            raise ValueError("poll_timeout_seconds must be greater than zero")
        self._consumer = consumer
        self._writer = writer
        self._topics = tuple(topics)
        self._topic_tables = dict(topic_tables)
        self._batch_size = batch_size
        self._poll_timeout_seconds = poll_timeout_seconds
        self._readiness_file = readiness_file
        self._clock = clock
        self._monotonic = monotonic

    def _touch_readiness(self) -> None:
        self._readiness_file.parent.mkdir(parents=True, exist_ok=True)
        self._readiness_file.write_text(self._clock().astimezone(UTC).isoformat(), encoding="utf-8")

    def _commit(self, records: Sequence[ConsumedRecord]) -> None:
        offsets = _commit_offsets(records)
        result = self._consumer.commit(offsets=offsets, asynchronous=False)
        failures = [partition for partition in result or [] if partition.error is not None]
        if failures:
            details = ", ".join(f"{item.topic}[{item.partition}]={item.error}" for item in failures)
            raise CdcConsumerError(f"synchronous Kafka offset commit failed: {details}")

    def run(
        self,
        *,
        should_stop: Callable[[], bool] = lambda: False,
        max_batches: int | None = None,
        max_messages: int | None = None,
        idle_timeout_seconds: float | None = None,
    ) -> int:
        """Consume until signalled or a deterministic test bound is reached."""
        if max_batches is not None and max_batches <= 0:
            raise ValueError("max_batches must be greater than zero")
        if max_messages is not None and max_messages <= 0:
            raise ValueError("max_messages must be greater than zero")
        if idle_timeout_seconds is not None and idle_timeout_seconds <= 0:
            raise ValueError("idle_timeout_seconds must be greater than zero")

        consumed = 0
        batches = 0
        idle_since = self._monotonic()
        self._writer.ensure_table()
        # confluent-kafka's C extension requires a concrete list, not a tuple.
        self._consumer.subscribe(list(self._topics))
        try:
            while not should_stop():
                request_size = self._batch_size
                if max_messages is not None:
                    request_size = min(request_size, max_messages - consumed)
                    if request_size <= 0:
                        break
                records = self._consumer.consume(
                    num_messages=request_size, timeout=self._poll_timeout_seconds
                )
                self._touch_readiness()
                if not records:
                    if (
                        idle_timeout_seconds is not None
                        and self._monotonic() - idle_since >= idle_timeout_seconds
                    ):
                        break
                    continue
                idle_since = self._monotonic()
                errors = [record.error() for record in records if record.error() is not None]
                if errors:
                    raise CdcConsumerError(f"Kafka poll returned record errors: {errors}")

                parsed: dict[str, CdcEvent] = {}
                ingested_at = self._clock()
                for record in records:
                    event = parse_debezium_record(
                        record, topic_tables=self._topic_tables, ingested_at=ingested_at
                    )
                    parsed.setdefault(event.event_id, event)
                events = list(parsed.values())
                self._writer.write(events)
                self._commit(records)
                consumed += len(records)
                batches += 1
                batch_log = context_logger(
                    __name__,
                    event_count=len(events),
                    kafka_record_count=len(records),
                    first_event_id=events[0].event_id,
                    last_event_id=events[-1].event_id,
                )
                batch_log.info("Kafka offsets committed after durable Bronze write")
                if max_batches is not None and batches >= max_batches:
                    break
            return consumed
        finally:
            self._consumer.close()
            self._readiness_file.unlink(missing_ok=True)


def as_kafka_consumer(value: object) -> KafkaConsumer:
    """Narrow a confluent-kafka Consumer to the protocol used by the runner."""
    return cast(KafkaConsumer, value)
