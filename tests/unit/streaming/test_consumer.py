"""Offset-after-Iceberg delivery-boundary tests for the CDC consumer."""

# pyright: reportMissingImports=false

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pytest
import trino
from confluent_kafka import TopicPartition

from omni_retail.streaming.cdc.consumer import CdcBatchWriter, CdcRunner
from omni_retail.streaming.cdc.model import CdcEvent

TOPIC = "omni.oltp.public.customers"
NOW = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)


@dataclass
class FakeMessage:
    offset_number: int = 1

    def topic(self) -> str:
        return TOPIC

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self.offset_number

    def key(self) -> bytes:
        return b'{"customer_id":9}'

    def value(self) -> bytes:
        return json.dumps(
            {
                "before": None,
                "after": {"customer_id": 9, "email": "cdc@example.test"},
                "source": {
                    "schema": "public",
                    "table": "customers",
                    "lsn": 99,
                    "txId": 8,
                    "ts_ms": 1_780_228_800_000,
                },
                "op": "c",
            }
        ).encode()

    def timestamp(self) -> tuple[int, int]:
        return (1, 1_780_228_800_000)

    def error(self) -> None:
        return None


class FakeConsumer:
    def __init__(self, batches: list[list[FakeMessage]], actions: list[str]) -> None:
        self.batches = batches
        self.actions = actions
        self.commits: list[list[TopicPartition]] = []

    def subscribe(self, topics: Sequence[str]) -> None:
        assert list(topics) == [TOPIC]
        self.actions.append("subscribe")

    def consume(self, num_messages: int, timeout: float) -> list[FakeMessage]:
        assert num_messages > 0
        assert timeout > 0
        return self.batches.pop(0) if self.batches else []

    def commit(
        self, *, offsets: list[TopicPartition], asynchronous: bool
    ) -> list[TopicPartition] | None:
        assert asynchronous is False
        self.actions.append("commit")
        self.commits.append(offsets)
        return None

    def close(self) -> None:
        self.actions.append("close")


class FakeWriter:
    def __init__(self, actions: list[str], *, fail: bool = False) -> None:
        self.actions = actions
        self.fail = fail
        self.event_ids: set[str] = set()

    def ensure_table(self) -> None:
        self.actions.append("ensure")

    def write(self, events: Sequence[CdcEvent]) -> None:
        self.actions.append("write")
        if self.fail:
            raise RuntimeError("Iceberg unavailable")
        self.event_ids.update(event.event_id for event in events)


def runner(consumer: FakeConsumer, writer: FakeWriter, readiness_file: Path) -> CdcRunner:
    return CdcRunner(
        consumer,
        writer,
        topics=(TOPIC,),
        topic_tables={TOPIC: "customers"},
        batch_size=10,
        poll_timeout_seconds=0.1,
        readiness_file=readiness_file,
        clock=lambda: NOW,
    )


def test_durable_write_happens_before_synchronous_offset_commit(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    actions: list[str] = []
    consumer = FakeConsumer([[FakeMessage()]], actions)
    writer = FakeWriter(actions)

    with caplog.at_level(logging.INFO):
        count = runner(consumer, writer, tmp_path / "ready").run(max_batches=1)

    assert count == 1
    assert actions == ["ensure", "subscribe", "write", "commit", "close"]
    assert consumer.commits[0][0].offset == 2
    assert "first_event_id=" in caplog.text


def test_write_failure_never_advances_offsets(tmp_path: Path) -> None:
    actions: list[str] = []
    consumer = FakeConsumer([[FakeMessage()]], actions)
    writer = FakeWriter(actions, fail=True)

    with pytest.raises(RuntimeError, match="Iceberg unavailable"):
        runner(consumer, writer, tmp_path / "ready").run(max_batches=1)

    assert "commit" not in actions
    assert actions[-1] == "close"


def test_replayed_transport_coordinate_is_a_sink_noop(tmp_path: Path) -> None:
    actions: list[str] = []
    writer = FakeWriter(actions)
    first = FakeConsumer([[FakeMessage()]], actions)
    second = FakeConsumer([[FakeMessage()]], actions)

    runner(first, writer, tmp_path / "ready-1").run(max_batches=1)
    runner(second, writer, tmp_path / "ready-2").run(max_batches=1)

    assert len(writer.event_ids) == 1
    assert actions.count("commit") == 2


class FlakyExecutor:
    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.parameterized_calls = 0

    def execute(self, sql: str) -> None:
        del sql

    def execute_with_params(self, sql: str, params: list[object]) -> None:
        assert "when not matched" in sql.lower()
        assert params
        self.parameterized_calls += 1
        if self.parameterized_calls <= self.failures:
            raise trino.exceptions.TrinoConnectionError("temporary Trino outage")

    def fetch(self, sql: str) -> list[tuple[object, ...]]:
        del sql
        return []


def parsed_event() -> CdcEvent:
    message = FakeMessage()
    from omni_retail.streaming.cdc.model import parse_debezium_record

    return parse_debezium_record(message, topic_tables={TOPIC: "customers"}, ingested_at=NOW)


def test_writer_retries_only_up_to_the_configured_bound() -> None:
    executor = FlakyExecutor(failures=2)
    delays: list[float] = []
    writer = CdcBatchWriter(
        executor, catalog="iceberg", schema="bronze", attempts=3, sleep=delays.append
    )

    writer.write([parsed_event()])

    assert executor.parameterized_calls == 3
    assert delays == [1.0, 2.0]


def test_writer_does_not_retry_non_transient_failures() -> None:
    class PermanentExecutor(FlakyExecutor):
        def execute_with_params(self, sql: str, params: list[object]) -> None:
            del sql, params
            self.parameterized_calls += 1
            raise ValueError("bad CDC payload")

    executor = PermanentExecutor(failures=0)
    writer = CdcBatchWriter(executor, catalog="iceberg", schema="bronze", attempts=3)

    with pytest.raises(ValueError, match="bad CDC payload"):
        writer.write([parsed_event()])
    assert executor.parameterized_calls == 1
