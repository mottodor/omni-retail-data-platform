"""Idempotently reconcile the Debezium PostgreSQL connector without logging secrets."""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

CONNECTOR_NAME = "omni-postgres-cdc"
CONNECTOR_TIMEOUT_SECONDS = 120.0
POLL_INTERVAL_SECONDS = 2.0
FAILED_STABILITY_POLLS = 2
FAILED_GRACE_SECONDS = 4.0
RUNBOOK = "docs/runbooks/kafka-cdc.md"


@dataclass(frozen=True)
class ConnectorTaskStatus:
    """One task's non-secret state and diagnostic trace."""

    task_id: int
    state: str
    trace: str | None


@dataclass(frozen=True)
class ConnectorStatus:
    """Parsed non-secret connector status used by the reconciler."""

    connector_state: str
    connector_trace: str | None
    tasks: tuple[ConnectorTaskStatus, ...]

    @property
    def is_running(self) -> bool:
        return (
            self.connector_state == "RUNNING"
            and bool(self.tasks)
            and all(task.state == "RUNNING" for task in self.tasks)
        )

    @property
    def summary(self) -> str:
        task_states = [f"{task.task_id}:{task.state}" for task in self.tasks]
        return f"connector={self.connector_state} tasks={task_states}"

    @property
    def failure_fingerprint(self) -> tuple[object, ...] | None:
        failed_tasks = tuple(
            (task.task_id, task.state, task.trace) for task in self.tasks if task.state == "FAILED"
        )
        connector_failed = self.connector_state == "FAILED"
        traces_present = bool(self.connector_trace) or any(task[2] for task in failed_tasks)
        if not traces_present or (not connector_failed and not failed_tasks):
            return None
        return (
            self.connector_state,
            self.connector_trace,
            failed_tasks,
        )

    @property
    def failure_traces(self) -> tuple[str, ...]:
        traces = [self.connector_trace] if self.connector_trace else []
        traces.extend(task.trace for task in self.tasks if task.state == "FAILED" and task.trace)
        return tuple(traces)


class ConnectorStatusError(RuntimeError):
    """Kafka Connect returned a malformed status payload."""


class ConnectorTerminalError(RuntimeError):
    """The connector remained in a stable terminal failure state."""


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"{name} is required")
    return value


def request_json(
    url: str,
    *,
    method: str = "GET",
    payload: object | None = None,
    allow_not_found: bool = False,
) -> object | None:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310
            return cast(object, json.loads(response.read().decode("utf-8")))
    except urllib.error.HTTPError as exc:
        if exc.code == 404 and allow_not_found:
            return None
        # Connect can echo submitted configuration in error responses. Do not
        # include the body because it can contain the replication password.
        raise RuntimeError(f"Kafka Connect request failed with HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError("Kafka Connect request failed") from exc


def connector_config() -> dict[str, str]:
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": required("DEBEZIUM_POSTGRES_USER"),
        "database.password": required("DEBEZIUM_POSTGRES_PASSWORD"),
        "database.dbname": required("POSTGRES_DB"),
        "plugin.name": "pgoutput",
        "topic.prefix": "omni.oltp",
        "slot.name": "omni_cdc_slot",
        "publication.name": "omni_cdc_publication",
        "publication.autocreate.mode": "disabled",
        "table.include.list": "public.customers,public.orders,public.payments",
        "snapshot.mode": "initial",
        "decimal.handling.mode": "string",
        "provide.transaction.metadata": "true",
        "heartbeat.interval.ms": "10000",
        "tombstones.on.delete": "false",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        # Kafka Connect 4.3 requires the default creation group even when the
        # allow-listed topics are pre-created by kafka-topics-init.
        "topic.creation.default.replication.factor": "1",
        "topic.creation.default.partitions": "1",
        "topic.creation.default.cleanup.policy": "delete",
        "topic.creation.default.retention.ms": "604800000",
        "topic.creation.default.retention.bytes": "5368709120",
    }


def _optional_trace(value: object) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def parse_connector_status(payload: object) -> ConnectorStatus:
    """Parse Connect's status response without exposing configuration values."""
    if not isinstance(payload, dict):
        raise ConnectorStatusError("Kafka Connect status response must be an object")
    connector = payload.get("connector")
    tasks = payload.get("tasks")
    if not isinstance(connector, dict) or not isinstance(tasks, list):
        raise ConnectorStatusError("Kafka Connect status response has invalid shape")
    connector_state = connector.get("state")
    if not isinstance(connector_state, str):
        raise ConnectorStatusError("Kafka Connect connector state is missing")

    parsed_tasks: list[ConnectorTaskStatus] = []
    for task in tasks:
        if not isinstance(task, dict):
            raise ConnectorStatusError("Kafka Connect task status must be an object")
        task_id = task.get("id")
        state = task.get("state")
        if isinstance(task_id, bool) or not isinstance(task_id, int):
            raise ConnectorStatusError("Kafka Connect task id is missing")
        if not isinstance(state, str):
            raise ConnectorStatusError("Kafka Connect task state is missing")
        parsed_tasks.append(
            ConnectorTaskStatus(
                task_id=task_id,
                state=state,
                trace=_optional_trace(task.get("trace")),
            )
        )
    return ConnectorStatus(
        connector_state=connector_state,
        connector_trace=_optional_trace(connector.get("trace")),
        tasks=tuple(sorted(parsed_tasks, key=lambda task: task.task_id)),
    )


def _stale_source_state(traces: tuple[str, ...]) -> bool:
    combined = "\n".join(traces).lower()
    unavailable_change_stream = (
        "change stream starting at" in combined and "no longer available" in combined
    )
    removed_wal = "requested wal segment" in combined and "has already been removed" in combined
    missing_slot = "replication slot" in combined and "does not exist" in combined
    invalid_lsn = "lsn" in combined and (
        "no longer available" in combined or "not available" in combined
    )
    return unavailable_change_stream or removed_wal or missing_slot or invalid_lsn


def _terminal_message(status: ConnectorStatus) -> str:
    if _stale_source_state(status.failure_traces):
        return (
            "Debezium connector failed because persisted Kafka Connect source offsets "
            "are incompatible with the current PostgreSQL WAL/replication-slot state. "
            f"Follow {RUNBOOK}. `make streaming-reset` is destructive and must be "
            "explicitly approved; no reset was performed."
        )
    return (
        "Debezium connector remained in terminal FAILED state. "
        f"Inspect bounded Connect logs and follow {RUNBOOK}; no reset was performed."
    )


def wait_for_running(
    read_status: Callable[[], object | None],
    *,
    timeout_seconds: float = CONNECTOR_TIMEOUT_SECONDS,
    poll_interval_seconds: float = POLL_INTERVAL_SECONDS,
    failed_stability_polls: int = FAILED_STABILITY_POLLS,
    failed_grace_seconds: float = FAILED_GRACE_SECONDS,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> ConnectorStatus:
    """Wait through transient startup but fail early on a stable terminal trace."""
    if timeout_seconds <= 0 or poll_interval_seconds <= 0:
        raise ValueError("connector wait durations must be greater than zero")
    if failed_stability_polls < 2 or failed_grace_seconds < 0:
        raise ValueError("terminal failure detection requires two polls and a non-negative grace")

    started_at = monotonic()
    deadline = started_at + timeout_seconds
    last_summary = "connector status is not available"
    previous_failure: tuple[object, ...] | None = None
    stable_failure_polls = 0

    while True:
        payload = read_status()
        if payload is not None:
            status = parse_connector_status(payload)
            last_summary = status.summary
            if status.is_running:
                return status

            failure = status.failure_fingerprint
            if failure is None:
                previous_failure = None
                stable_failure_polls = 0
            elif failure == previous_failure:
                stable_failure_polls += 1
            else:
                previous_failure = failure
                stable_failure_polls = 1

            elapsed = monotonic() - started_at
            if stable_failure_polls >= failed_stability_polls and elapsed >= failed_grace_seconds:
                raise ConnectorTerminalError(_terminal_message(status))
        else:
            previous_failure = None
            stable_failure_polls = 0

        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"Debezium connector did not become RUNNING after "
                f"{timeout_seconds:g}s: {last_summary}"
            )
        sleep(min(poll_interval_seconds, remaining))


def reconcile() -> None:
    base_url = os.environ.get("KAFKA_CONNECT_URL", "http://debezium-connect:8083").rstrip("/")
    # PUT creates or updates in place. Never print the request body or response:
    # both contain the replication password.
    request_json(
        f"{base_url}/connectors/{CONNECTOR_NAME}/config",
        method="PUT",
        payload=connector_config(),
    )
    status_url = f"{base_url}/connectors/{CONNECTOR_NAME}/status"
    wait_for_running(lambda: request_json(status_url, allow_not_found=True))
    print(f"Debezium connector {CONNECTOR_NAME} is RUNNING")


def main() -> int:
    try:
        reconcile()
    except (ConnectorStatusError, ConnectorTerminalError, RuntimeError, TimeoutError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
