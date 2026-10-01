"""Idempotently reconcile the Debezium PostgreSQL connector without logging secrets."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from typing import cast

CONNECTOR_NAME = "omni-postgres-cdc"


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
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Kafka Connect request failed: HTTP {exc.code}: {detail}") from exc


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


def reconcile() -> None:
    base_url = os.environ.get("KAFKA_CONNECT_URL", "http://debezium-connect:8083").rstrip("/")
    # PUT creates or updates in place. Never print the request body: it contains
    # the replication password.
    request_json(
        f"{base_url}/connectors/{CONNECTOR_NAME}/config",
        method="PUT",
        payload=connector_config(),
    )
    deadline = time.monotonic() + 120
    last_state = "unknown"
    while time.monotonic() < deadline:
        status = request_json(
            f"{base_url}/connectors/{CONNECTOR_NAME}/status", allow_not_found=True
        )
        if isinstance(status, dict):
            connector = status.get("connector")
            tasks = status.get("tasks")
            connector_state = connector.get("state") if isinstance(connector, dict) else None
            task_states = (
                [task.get("state") for task in tasks if isinstance(task, dict)]
                if isinstance(tasks, list)
                else []
            )
            last_state = f"connector={connector_state} tasks={task_states}"
            if (
                connector_state == "RUNNING"
                and task_states
                and all(state == "RUNNING" for state in task_states)
            ):
                print(f"Debezium connector {CONNECTOR_NAME} is RUNNING")
                return
            # A PUT that replaces a previously failed connector can expose the
            # old FAILED status briefly while the distributed worker converges.
            # Keep bounded polling and report the final state at the deadline.
        time.sleep(2)
    raise TimeoutError(f"Debezium connector did not become RUNNING: {last_state}")


if __name__ == "__main__":
    reconcile()
