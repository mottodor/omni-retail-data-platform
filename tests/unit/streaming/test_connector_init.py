"""Fail-fast, secret-free Debezium connector reconciliation tests."""

import importlib.util
import sys
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

SCRIPT_PATH = (
    Path(__file__).resolve().parents[3]
    / "infrastructure"
    / "scripts"
    / "debezium_connector_init.py"
)
SPEC = importlib.util.spec_from_file_location("debezium_connector_init_under_test", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
connector_init: Any = importlib.util.module_from_spec(SPEC)
assert isinstance(connector_init, ModuleType)
sys.modules[SPEC.name] = connector_init
SPEC.loader.exec_module(connector_init)


class FakeTime:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class StatusReader:
    def __init__(self, statuses: Sequence[object | None]) -> None:
        self._statuses = list(statuses)
        self._last = self._statuses[-1]
        self.calls = 0

    def __call__(self) -> object | None:
        self.calls += 1
        return self._statuses.pop(0) if self._statuses else self._last


def status(
    connector_state: str = "RUNNING",
    task_state: str = "RUNNING",
    *,
    trace: str | None = None,
) -> dict[str, object]:
    task: dict[str, object] = {"id": 0, "state": task_state}
    if trace is not None:
        task["trace"] = trace
    return {
        "name": "omni-postgres-cdc",
        "connector": {"state": connector_state, "worker_id": "worker"},
        "tasks": [task],
    }


def wait(
    statuses: Sequence[object | None],
    *,
    timeout: float = 10.0,
    grace: float = 2.0,
) -> tuple[Any, FakeTime, StatusReader]:
    timeline = FakeTime()
    reader = StatusReader(statuses)
    observed = connector_init.wait_for_running(
        reader,
        timeout_seconds=timeout,
        poll_interval_seconds=1.0,
        failed_stability_polls=2,
        failed_grace_seconds=grace,
        monotonic=timeline.monotonic,
        sleep=timeline.sleep,
    )
    return observed, timeline, reader


def test_parse_and_wait_return_immediately_for_healthy_connector() -> None:
    observed, timeline, reader = wait([status()])

    assert observed.is_running is True
    assert observed.summary == "connector=RUNNING tasks=['0:RUNNING']"
    assert reader.calls == 1
    assert timeline.sleeps == []


def test_transient_startup_states_remain_bounded_and_retryable() -> None:
    observed, timeline, reader = wait(
        [
            None,
            status("UNASSIGNED", "UNASSIGNED"),
            status("PAUSED", "PAUSED"),
            status(),
        ]
    )

    assert observed.is_running is True
    assert reader.calls == 4
    assert timeline.sleeps == [1.0, 1.0, 1.0]


def test_stable_terminal_failure_stops_before_full_timeout() -> None:
    failed = status(
        task_state="FAILED",
        trace="org.example.ConnectException: source task failed",
    )

    with pytest.raises(connector_init.ConnectorTerminalError, match="terminal FAILED"):
        wait([failed], timeout=120.0)


def test_unavailable_wal_failure_has_actionable_destructive_warning() -> None:
    failed = status(
        task_state="FAILED",
        trace=(
            "DebeziumException: change stream starting at 0/F404830 "
            "is no longer available on the server"
        ),
    )

    with pytest.raises(connector_init.ConnectorTerminalError) as raised:
        wait([failed], timeout=120.0)

    message = str(raised.value)
    assert "persisted Kafka Connect source offsets" in message
    assert "PostgreSQL WAL/replication-slot state" in message
    assert "docs/runbooks/kafka-cdc.md" in message
    assert "streaming-reset` is destructive" in message
    assert "no reset was performed" in message


def test_failure_message_never_echoes_trace_or_credentials() -> None:
    secret = "replication-password-do-not-print"
    failed = status(
        task_state="FAILED",
        trace=(
            "change stream starting at 0/F404830 is no longer available; "
            f"database.password={secret}"
        ),
    )

    with pytest.raises(connector_init.ConnectorTerminalError) as raised:
        wait([failed], timeout=120.0)

    assert secret not in str(raised.value)
    assert "database.password" not in str(raised.value)


def test_transient_state_times_out_with_state_summary() -> None:
    with pytest.raises(TimeoutError, match="connector=PAUSED tasks=\\['0:PAUSED'\\]"):
        wait([status("PAUSED", "PAUSED")], timeout=2.5)


@pytest.mark.parametrize(
    "payload",
    [None, [], {"connector": {}}, {"connector": {"state": "RUNNING"}, "tasks": [{}]}],
)
def test_status_parser_rejects_malformed_payload(payload: object) -> None:
    if payload is None:
        with pytest.raises(connector_init.ConnectorStatusError):
            connector_init.parse_connector_status(payload)
    else:
        with pytest.raises(connector_init.ConnectorStatusError):
            connector_init.parse_connector_status(payload)
