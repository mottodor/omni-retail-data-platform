"""Hermetic checks for optional live-integration prerequisite classification."""

# pyright: reportMissingImports=false

from collections.abc import Callable

import httpx
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError as ClickHouseDatabaseError
from trino.exceptions import TrinoExternalError, TrinoUserError

from integration.prerequisites import (
    OptionalServiceUnavailableError,
    is_missing_clickhouse_relation,
    is_missing_trino_relation,
    kafka_connector_is_running,
    require_clickhouse_rows,
    require_kafka_connector_running,
    require_superset_health,
    require_trino_rows,
)


def trino_error(name: str, message: str = "query failed") -> TrinoUserError:
    return TrinoUserError(
        {
            "errorType": "USER_ERROR",
            "errorName": name,
            "message": message,
        },
        "test-query",
    )


def raising_count(error: Exception) -> Callable[[], object]:
    def raise_error() -> object:
        raise error

    return raise_error


def test_unreachable_streaming_profile_skips_with_actionable_setup() -> None:
    def unavailable() -> object:
        raise OptionalServiceUnavailableError("connection refused")

    with pytest.raises(pytest.skip.Exception, match="make streaming-up"):
        require_kafka_connector_running(unavailable)


def test_reachable_unhealthy_connector_fails_instead_of_skipping() -> None:
    status = {
        "connector": {"state": "RUNNING"},
        "tasks": [{"id": 0, "state": "FAILED"}],
    }

    assert not kafka_connector_is_running(status)
    with pytest.raises(AssertionError, match="reachable but.*unhealthy"):
        require_kafka_connector_running(lambda: status)


def test_healthy_connector_status_is_accepted() -> None:
    status = {
        "connector": {"state": "RUNNING"},
        "tasks": [{"id": 0, "state": "RUNNING"}],
    }

    assert kafka_connector_is_running(status)
    require_kafka_connector_running(lambda: status)


def test_unexpected_connector_probe_error_propagates() -> None:
    error = ValueError("invalid status JSON")

    def invalid_response() -> object:
        raise error

    with pytest.raises(ValueError) as raised:
        require_kafka_connector_running(invalid_response)
    assert raised.value is error


def test_unreachable_superset_skips_with_actionable_setup() -> None:
    request = httpx.Request("GET", "http://127.0.0.1:8088/health")

    def refused(*args: object, **kwargs: object) -> httpx.Response:
        del args, kwargs
        raise httpx.ConnectError("connection refused", request=request)

    with pytest.raises(pytest.skip.Exception, match="make bi-up"):
        require_superset_health(
            "http://127.0.0.1:8088",
            request=refused,
            timeout_seconds=1.0,
        )


def test_reachable_superset_read_timeout_propagates() -> None:
    request = httpx.Request("GET", "http://127.0.0.1:8088/health")
    error = httpx.ReadTimeout("health response timed out", request=request)

    def timed_out(*args: object, **kwargs: object) -> httpx.Response:
        del args, kwargs
        raise error

    with pytest.raises(httpx.ReadTimeout) as raised:
        require_superset_health(
            "http://127.0.0.1:8088",
            request=timed_out,
            timeout_seconds=1.0,
        )
    assert raised.value is error


def test_reachable_unhealthy_superset_fails_instead_of_skipping() -> None:
    response = httpx.Response(503, text="starting")

    with pytest.raises(AssertionError, match="reachable but unhealthy"):
        require_superset_health(
            "http://127.0.0.1:8088",
            request=lambda *args, **kwargs: response,
            timeout_seconds=1.0,
        )


def test_healthy_superset_response_is_returned() -> None:
    response = httpx.Response(200, text="OK")

    assert (
        require_superset_health(
            "http://127.0.0.1:8088",
            request=lambda *args, **kwargs: response,
            timeout_seconds=1.0,
        )
        is response
    )


@pytest.mark.parametrize("error_name", ["SCHEMA_NOT_FOUND", "TABLE_NOT_FOUND"])
def test_trino_missing_relation_is_classified(error_name: str) -> None:
    assert is_missing_trino_relation(trino_error(error_name))


def test_unexpected_trino_error_is_not_a_missing_prerequisite() -> None:
    error = TrinoExternalError(
        {
            "errorType": "EXTERNAL",
            "errorName": "ICEBERG_CATALOG_ERROR",
            "message": "catalog unavailable",
        },
        "test-query",
    )
    assert not is_missing_trino_relation(error)

    with pytest.raises(TrinoExternalError) as raised:
        require_trino_rows(
            raising_count(error),
            dataset="Iceberg analytics.mart_daily_sales",
            setup_command="make dbt-build",
        )
    assert raised.value is error


@pytest.mark.parametrize(
    "loader",
    [
        pytest.param(raising_count(trino_error("TABLE_NOT_FOUND")), id="missing"),
        pytest.param(lambda: 0, id="empty"),
    ],
)
def test_missing_or_empty_trino_dataset_skips(loader: Callable[[], object]) -> None:
    with pytest.raises(pytest.skip.Exception, match="make dbt-build"):
        require_trino_rows(
            loader,
            dataset="Iceberg analytics.mart_daily_sales",
            setup_command="make dbt-build",
        )


def test_nonempty_trino_dataset_returns_count() -> None:
    assert (
        require_trino_rows(
            lambda: 7,
            dataset="Iceberg analytics.mart_daily_sales",
            setup_command="make dbt-build",
        )
        == 7
    )


@pytest.mark.parametrize("code", [60, 81])
def test_clickhouse_missing_relation_is_classified(code: int) -> None:
    assert is_missing_clickhouse_relation(ClickHouseDatabaseError("missing", code=code))


def test_clickhouse_auth_error_is_not_a_missing_prerequisite() -> None:
    error = ClickHouseDatabaseError("authentication failed", code=516)
    assert not is_missing_clickhouse_relation(error)

    with pytest.raises(ClickHouseDatabaseError) as raised:
        require_clickhouse_rows(
            raising_count(error),
            dataset="ClickHouse analytics.mart_daily_sales",
            setup_command="make dbt-build && make serving-rebuild",
        )
    assert raised.value is error


@pytest.mark.parametrize(
    "loader",
    [
        pytest.param(
            raising_count(ClickHouseDatabaseError("unknown table", code=60)),
            id="missing",
        ),
        pytest.param(lambda: 0, id="empty"),
    ],
)
def test_missing_or_empty_clickhouse_dataset_skips(loader: Callable[[], object]) -> None:
    with pytest.raises(pytest.skip.Exception, match="make serving-rebuild"):
        require_clickhouse_rows(
            loader,
            dataset="ClickHouse analytics.mart_daily_sales",
            setup_command="make dbt-build && make serving-rebuild",
        )


def test_nonempty_clickhouse_dataset_returns_count() -> None:
    assert (
        require_clickhouse_rows(
            lambda: 11,
            dataset="ClickHouse analytics.mart_daily_sales",
            setup_command="make dbt-build && make serving-rebuild",
        )
        == 11
    )
