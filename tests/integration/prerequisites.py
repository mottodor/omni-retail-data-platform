"""Focused prerequisite checks for optional live integration boundaries."""

from collections.abc import Callable, Mapping

import httpx
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError as ClickHouseDatabaseError
from trino.exceptions import Error as TrinoError
from trino.exceptions import TrinoQueryError

TRINO_MISSING_RELATION_ERRORS = frozenset({"SCHEMA_NOT_FOUND", "TABLE_NOT_FOUND"})
CLICKHOUSE_MISSING_RELATION_CODES = frozenset({60, 81})


class OptionalServiceUnavailableError(ConnectionError):
    """An optional service endpoint could not be reached at transport level."""


def kafka_connector_is_running(status: object) -> bool:
    """Whether a Kafka Connect status payload reports the connector and all tasks running."""
    if not isinstance(status, Mapping):
        return False
    connector = status.get("connector")
    tasks = status.get("tasks")
    return (
        isinstance(connector, Mapping)
        and connector.get("state") == "RUNNING"
        and isinstance(tasks, list)
        and bool(tasks)
        and all(isinstance(task, Mapping) and task.get("state") == "RUNNING" for task in tasks)
    )


def require_kafka_connector_running(load_status: Callable[[], object]) -> None:
    """Skip an absent streaming endpoint but fail for a reachable unhealthy connector."""
    try:
        status = load_status()
    except OptionalServiceUnavailableError as error:
        pytest.skip(
            f"Kafka Connect is not reachable ({type(error).__name__}); "
            "start the streaming profile: `make streaming-up`"
        )
    assert kafka_connector_is_running(status), (
        f"Kafka Connect is reachable but the CDC connector is unhealthy: {status!r}"
    )


def require_superset_health(
    base_url: str,
    *,
    request: Callable[..., httpx.Response] = httpx.get,
    timeout_seconds: float,
) -> httpx.Response:
    """Return a healthy response, skipping only when Superset is unreachable."""
    try:
        response = request(f"{base_url}/health", timeout=timeout_seconds)
    except (httpx.ConnectError, httpx.ConnectTimeout) as error:
        pytest.skip(f"Superset is not reachable ({error}); start the BI profile: `make bi-up`")
    assert response.status_code == 200, (
        f"Superset is reachable but unhealthy: {response.status_code} {response.text}"
    )
    assert response.text.startswith("OK"), (
        f"Superset health endpoint returned an unexpected body: {response.text!r}"
    )
    return response


def is_missing_trino_relation(error: BaseException) -> bool:
    """Whether Trino explicitly reports an absent schema or table."""
    return isinstance(error, TrinoQueryError) and error.error_name in TRINO_MISSING_RELATION_ERRORS


def is_missing_clickhouse_relation(error: BaseException) -> bool:
    """Whether ClickHouse explicitly reports an absent database or table."""
    return (
        isinstance(error, ClickHouseDatabaseError)
        and error.code in CLICKHOUSE_MISSING_RELATION_CODES
    )


def require_trino_rows(
    query_count: Callable[[], object],
    *,
    dataset: str,
    setup_command: str,
) -> int:
    """Require a non-empty Trino dataset; propagate every unexpected query error."""
    try:
        count = query_count()
    except TrinoError as error:
        if is_missing_trino_relation(error):
            pytest.skip(f"{dataset} is missing; prepare it with `{setup_command}`")
        raise
    assert isinstance(count, int), f"{dataset} count must be an integer, got {count!r}"
    if count <= 0:
        pytest.skip(f"{dataset} is empty; prepare it with `{setup_command}`")
    return count


def require_clickhouse_rows(
    query_count: Callable[[], object],
    *,
    dataset: str,
    setup_command: str,
) -> int:
    """Require a non-empty serving dataset; propagate non-prerequisite failures."""
    try:
        count = query_count()
    except ClickHouseDatabaseError as error:
        if is_missing_clickhouse_relation(error):
            pytest.skip(f"{dataset} is missing; prepare it with `{setup_command}`")
        raise
    assert isinstance(count, int), f"{dataset} count must be an integer, got {count!r}"
    if count <= 0:
        pytest.skip(f"{dataset} is empty; prepare it with `{setup_command}`")
    return count
