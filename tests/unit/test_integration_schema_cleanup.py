"""Hermetic checks for disposable Iceberg schema teardown."""

# pyright: reportMissingImports=false

import pytest
from trino.exceptions import TrinoExternalError, TrinoUserError

from integration.conftest import SchemaDropError, cleanup_schemas, drop_schema_with_retry


def transient_catalog_error(message: str = "Failed to load table: fixture") -> TrinoExternalError:
    return TrinoExternalError(
        {
            "errorType": "EXTERNAL",
            "errorName": "ICEBERG_CATALOG_ERROR",
            "message": message,
        },
        "test-query",
    )


def non_transient_error() -> TrinoUserError:
    return TrinoUserError(
        {
            "errorType": "USER_ERROR",
            "errorName": "SYNTAX_ERROR",
            "message": "bad drop statement",
        },
        "test-query",
    )


def test_schema_drop_succeeds_without_sleep() -> None:
    statements: list[str] = []
    sleeps: list[float] = []

    drop_schema_with_retry(
        "it_123_bronze",
        execute=statements.append,
        sleep=sleeps.append,
    )

    assert statements == ["drop schema if exists iceberg.it_123_bronze cascade"]
    assert sleeps == []


def test_schema_drop_recovers_from_transient_catalog_visibility() -> None:
    attempts = 0
    statements: list[str] = []
    sleeps: list[float] = []

    def execute(sql: str) -> None:
        nonlocal attempts
        attempts += 1
        statements.append(sql)
        if attempts < 3:
            raise transient_catalog_error()

    drop_schema_with_retry(
        "it_123_gold",
        execute=execute,
        sleep=sleeps.append,
    )

    assert len(statements) == 3
    assert len(set(statements)) == 1
    assert sleeps == [1.0, 2.0]


def test_schema_drop_reports_exhausted_transient_retry_context() -> None:
    error = transient_catalog_error("Failed to load table: fixture")
    calls = 0
    sleeps: list[float] = []

    def execute(sql: str) -> None:
        nonlocal calls
        del sql
        calls += 1
        raise error

    with pytest.raises(SchemaDropError) as raised:
        drop_schema_with_retry(
            "it_123_silver",
            execute=execute,
            sleep=sleeps.append,
        )

    assert calls == 3
    assert sleeps == [1.0, 2.0]
    assert raised.value.schema == "it_123_silver"
    assert raised.value.attempts == 3
    assert raised.value.error is error
    assert raised.value.__cause__ is error
    assert "error_type=TrinoExternalError" in str(raised.value)
    assert "Failed to load table: fixture" in str(raised.value)


def test_schema_drop_does_not_retry_non_transient_error() -> None:
    error = non_transient_error()
    calls = 0
    sleeps: list[float] = []

    def execute(sql: str) -> None:
        nonlocal calls
        del sql
        calls += 1
        raise error

    with pytest.raises(SchemaDropError) as raised:
        drop_schema_with_retry(
            "it_123_analytics",
            execute=execute,
            sleep=sleeps.append,
        )

    assert calls == 1
    assert sleeps == []
    assert raised.value.attempts == 1
    assert raised.value.error is error


def test_cleanup_uses_reverse_order_and_continues_after_failure() -> None:
    schemas = ("it_123_bronze", "it_123_silver", "it_123_gold", "it_123_analytics")
    visited: list[str] = []
    terminal_error = non_transient_error()

    def drop(schema: str) -> None:
        visited.append(schema)
        if schema == "it_123_gold":
            raise SchemaDropError(schema, 1, terminal_error)

    errors = cleanup_schemas(schemas, drop_schema=drop)

    assert visited == list(reversed(schemas))
    assert len(errors) == 1
    assert errors[0].schema == "it_123_gold"
    assert errors[0].error is terminal_error


def test_cleanup_aggregates_every_terminal_failure() -> None:
    schemas = ("it_123_bronze", "it_123_silver", "it_123_gold")

    def failing_drop(schema: str) -> None:
        raise SchemaDropError(schema, 3, transient_catalog_error(schema))

    errors = cleanup_schemas(schemas, drop_schema=failing_drop)

    assert [error.schema for error in errors] == list(reversed(schemas))
    assert all(error.attempts == 3 for error in errors)
    rendered = "; ".join(str(error) for error in errors)
    assert all(schema in rendered for schema in schemas)


@pytest.mark.parametrize("attempts", [0, -1])
def test_schema_drop_rejects_empty_retry_budget(attempts: int) -> None:
    def execute(sql: str) -> None:
        del sql

    with pytest.raises(ValueError, match="at least 1"):
        drop_schema_with_retry("it_123_bronze", execute=execute, attempts=attempts)
