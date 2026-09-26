"""Unit tests for the serving publisher: swap sequencing, failure isolation."""

from datetime import date
from decimal import Decimal

import pytest

from fakes.clickhouse import FakeClickHouseExecutor
from fakes.trino import FakeTrinoExecutor
from omni_retail.serving.clickhouse.publisher import (
    PublishError,
    exchange_tables_sql,
    publish,
    rebuild,
    snapshot_sql,
    truncate_staging_sql,
)
from omni_retail.serving.clickhouse.specs import MART_DAILY_SALES

SPEC = MART_DAILY_SALES

#: A two-row Gold snapshot as the trino client would return it
#: (date, int, str, Decimal columns in spec order).
ROWS: list[tuple[object, ...]] = [
    (
        20260918,
        date(2026, 9, 18),
        "electronics",
        "emea",
        2,
        5,
        Decimal("410.091743119"),
        Decimal("150.000000000"),
    ),
    (
        20260918,
        date(2026, 9, 18),
        "toys",
        "emea",
        1,
        2,
        Decimal("40.000000000"),
        Decimal("12.000000000"),
    ),
]


def trino_with_mart_rows() -> FakeTrinoExecutor:
    executor = FakeTrinoExecutor()
    executor.fetch_by_prefix = {'select "date_key"': ROWS}
    return executor


class TestSqlBuilders:
    def test_snapshot_sql_lists_columns_explicitly(self) -> None:
        sql = snapshot_sql(SPEC, "iceberg")
        assert sql == (
            'select "date_key", "order_date", "category_name", "region", '
            '"orders_count", "items_sold", "revenue_eur", "margin_eur" '
            "from iceberg.analytics.mart_daily_sales"
        )
        assert "*" not in sql

    def test_truncate_and_exchange_target_the_pair(self) -> None:
        assert truncate_staging_sql(SPEC) == "truncate table analytics.mart_daily_sales_staging"
        assert exchange_tables_sql(SPEC) == (
            "exchange tables analytics.mart_daily_sales and analytics.mart_daily_sales_staging"
        )


class TestPublish:
    def test_statement_sequence_is_truncate_insert_exchange(self) -> None:
        trino = trino_with_mart_rows()
        ch = FakeClickHouseExecutor()
        result = publish(trino, ch, SPEC)

        assert ch.commands == [
            "truncate table analytics.mart_daily_sales_staging",
            "exchange tables analytics.mart_daily_sales and analytics.mart_daily_sales_staging",
        ]
        assert len(ch.inserts) == 1
        table, columns, rows = ch.inserts[0]
        assert table == "analytics.mart_daily_sales_staging"
        assert columns == SPEC.column_names
        assert rows == ROWS
        assert (result.mart, result.mode, result.row_count) == (SPEC.name, "publish", len(ROWS))
        assert result.duration_seconds >= 0

    def test_serving_table_is_never_written_directly(self) -> None:
        ch = FakeClickHouseExecutor()
        publish(trino_with_mart_rows(), ch, SPEC)
        inserted_tables = {table for table, _, _ in ch.inserts}
        assert "analytics.mart_daily_sales" not in inserted_tables

    def test_publish_is_repetitive_by_construction(self) -> None:
        # Two consecutive publishes issue the identical swap sequence —
        # idempotency is structural (truncate + swap), not stateful.
        trino = trino_with_mart_rows()
        ch = FakeClickHouseExecutor()
        publish(trino, ch, SPEC)
        publish(trino, ch, SPEC)
        assert ch.commands[:2] == ch.commands[2:]
        assert len(ch.inserts) == 2

    def test_insert_failure_never_reaches_the_exchange(self) -> None:
        ch = FakeClickHouseExecutor(fail_inserts=True)
        with pytest.raises(RuntimeError, match="simulated"):
            publish(trino_with_mart_rows(), ch, SPEC)
        assert ch.commands_matching("exchange tables") == []

    def test_written_rows_mismatch_is_a_publish_error(self) -> None:
        ch = FakeClickHouseExecutor(written_rows_override=len(ROWS) + 1)
        with pytest.raises(PublishError, match="written row count mismatch"):
            publish(trino_with_mart_rows(), ch, SPEC)
        assert ch.commands_matching("exchange tables") == []


class TestRebuild:
    def test_rebuild_recreates_ddl_then_runs_the_standard_swap(self) -> None:
        ch = FakeClickHouseExecutor()
        result = rebuild(trino_with_mart_rows(), ch, SPEC)

        assert ch.commands[0].startswith("create table if not exists analytics.mart_daily_sales\n")
        assert ch.commands[1].startswith(
            "create table if not exists analytics.mart_daily_sales_staging\n"
        )
        assert ch.commands[2:] == [
            "truncate table analytics.mart_daily_sales_staging",
            "exchange tables analytics.mart_daily_sales and analytics.mart_daily_sales_staging",
        ]
        assert result.mode == "rebuild"
        assert result.row_count == len(ROWS)
