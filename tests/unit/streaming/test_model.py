"""Debezium envelope and transport-identity contract tests."""

# pyright: reportMissingImports=false

import json
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from omni_retail.streaming.cdc.model import (
    CdcRecordError,
    event_identity,
    parse_debezium_record,
)

TOPIC = "omni.oltp.public.orders"
NOW = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)


@dataclass
class FakeMessage:
    value_payload: bytes | None
    key_payload: bytes | None = b'{"order_id":42}'
    topic_name: str = TOPIC
    partition_number: int = 0
    offset_number: int = 7
    timestamp_ms: int = 1_780_228_800_000

    def topic(self) -> str:
        return self.topic_name

    def partition(self) -> int:
        return self.partition_number

    def offset(self) -> int:
        return self.offset_number

    def key(self) -> bytes | None:
        return self.key_payload

    def value(self) -> bytes | None:
        return self.value_payload

    def timestamp(self) -> tuple[int, int]:
        return (1, self.timestamp_ms)


def envelope(operation: str) -> bytes:
    before = {"order_id": 42, "order_total": "10.50"} if operation in {"u", "d"} else None
    after = (
        None
        if operation == "d"
        else {
            "order_id": 42,
            "order_total": "10.50",
            "updated_at": 1_780_228_800_123_456,
        }
    )
    return json.dumps(
        {
            "before": before,
            "after": after,
            "source": {
                "schema": "public",
                "table": "orders",
                "lsn": 123456,
                "txId": 91,
                "ts_us": 1_780_228_800_123_456,
            },
            "op": operation,
            "ts_ms": 1_780_228_801_000,
        },
        separators=(",", ":"),
    ).encode()


@pytest.mark.parametrize("operation", ["r", "c", "u", "d"])
def test_parse_supported_debezium_operations(operation: str) -> None:
    message = FakeMessage(envelope(operation))
    event = parse_debezium_record(message, topic_tables={TOPIC: "orders"}, ingested_at=NOW)

    assert event.operation == operation
    assert event.event_id == event_identity(TOPIC, 0, 7)
    assert event.source_lsn == 123456
    assert event.source_tx_id == 91
    assert event.source_timestamp == datetime(2026, 5, 31, 12, 0, 0, 123456, tzinfo=UTC)
    assert event.event_date.isoformat() == "2026-05-31"
    assert event.key_json == '{"order_id":42}'
    assert '"order_total":"10.50"' in event.envelope_json
    assert (event.after_json is None) is (operation == "d")


def test_event_identity_changes_with_each_transport_coordinate() -> None:
    identities = {
        event_identity(TOPIC, 0, 7),
        event_identity(TOPIC, 0, 8),
        event_identity(TOPIC, 1, 7),
    }
    assert len(identities) == 3
    assert all(len(identity) == 64 for identity in identities)


@pytest.mark.parametrize(
    ("message", "match"),
    [
        (FakeMessage(None), "broker tombstone"),
        (FakeMessage(b"not-json"), "not valid JSON"),
        (FakeMessage(envelope("c"), topic_name="omni.oltp.public.products"), "allow-list"),
        (
            FakeMessage(envelope("c").replace(b'"orders"', b'"payments"')),
            "route mismatch",
        ),
        (FakeMessage(envelope("x")), "unsupported Debezium operation"),
    ],
)
def test_invalid_records_fail_with_transport_context(message: FakeMessage, match: str) -> None:
    with pytest.raises(CdcRecordError, match=match) as exc_info:
        parse_debezium_record(message, topic_tables={TOPIC: "orders"}, ingested_at=NOW)
    assert "topic=omni.oltp.public" in str(exc_info.value)
    assert "partition=0 offset=7" in str(exc_info.value)
