"""Unit tests for Trino SQL literal rendering and batched INSERTs."""

from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from omni_retail.lakehouse.bronze.literals import insert_statements, sql_literal
from omni_retail.lakehouse.bronze.specs import TABLES


def test_varchar_literal_escapes_single_quotes() -> None:
    assert sql_literal("O'Brien", "varchar") == "'O''Brien'"


def test_none_renders_untyped_null() -> None:
    assert sql_literal(None, "varchar") == "null"


def test_boolean_literal() -> None:
    assert sql_literal(True, "boolean") == "true"
    assert sql_literal(False, "boolean") == "false"


def test_bigint_literal_and_bool_rejection() -> None:
    assert sql_literal(42, "bigint") == "42"
    with pytest.raises(TypeError):
        sql_literal(True, "bigint")
    with pytest.raises(TypeError):
        sql_literal("42", "bigint")


def test_decimal_literal_is_typed() -> None:
    assert sql_literal(Decimal("12.34"), "decimal(12,2)") == "DECIMAL '12.34'"
    with pytest.raises(TypeError):
        sql_literal(12.34, "decimal(12,2)")


def test_double_literal() -> None:
    assert sql_literal(1.5, "double") == "1.5"


def test_timestamp_literal_normalizes_to_utc() -> None:
    value = datetime(2026, 9, 18, 12, 0, tzinfo=timezone(timedelta(hours=2)))
    rendered = sql_literal(value, "timestamp(6) with time zone")
    assert rendered == "TIMESTAMP '2026-09-18 10:00:00.000000 UTC'"


def test_naive_datetime_is_assumed_utc() -> None:
    value = datetime(2026, 9, 18, 8, 30, 1, 500000)
    assert sql_literal(value, "timestamp(6) with time zone") == (
        "TIMESTAMP '2026-09-18 08:30:01.500000 UTC'"
    )


def test_date_literal() -> None:
    assert sql_literal(date(2026, 9, 18), "date") == "DATE '2026-09-18'"


def test_unknown_trino_type_raises() -> None:
    with pytest.raises(TypeError, match="unsupported trino type"):
        sql_literal(1, "row(varchar)")


def categories_row(category_id: int) -> dict[str, object]:
    return {
        "category_id": category_id,
        "name": f"cat-{category_id}",
        "parent_category_id": None,
        "created_at": datetime(2026, 9, 1, tzinfo=UTC),
        "updated_at": datetime(2026, 9, 1, tzinfo=UTC),
        "_batch_id": "postgres-categories-20260918",
        "_batch_date": date(2026, 9, 18),
        "_source_object": "postgres/categories/2026/09/18/data.parquet",
        "_ingested_at": datetime(2026, 9, 18, 9, 0, tzinfo=UTC),
    }


def test_insert_statements_batches_rows() -> None:
    spec = TABLES["categories"]
    rows = [categories_row(index) for index in range(1001)]
    statements = insert_statements(spec, rows, rows_per_statement=500)
    assert len(statements) == 3
    assert statements[0].startswith('insert into iceberg.bronze.categories ("category_id"')
    assert statements[0].count("), (") == 499
    assert statements[1].count("), (") == 499
    assert statements[2].count("), (") == 0


def test_insert_statement_contains_all_columns_in_order() -> None:
    spec = TABLES["categories"]
    statements = insert_statements(spec, [categories_row(1)])
    expected_columns = ", ".join(f'"{c.name}"' for c in spec.all_columns)
    assert f"({expected_columns}) values" in statements[0]


def test_insert_statements_reject_missing_columns() -> None:
    with pytest.raises(ValueError, match="missing columns"):
        insert_statements(TABLES["categories"], [{"category_id": 1}])
