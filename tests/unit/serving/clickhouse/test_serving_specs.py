"""Unit tests for the serving mart specs and the migration cross-check."""

import re
from pathlib import Path

import pytest

from omni_retail.serving.clickhouse.specs import (
    MART_DAILY_SALES,
    MARTS,
    MartColumnSpec,
    MartSpec,
    mart_by_name,
)

REPO_ROOT = Path(__file__).resolve().parents[4]
MIGRATION_0003 = REPO_ROOT / "clickhouse" / "migrations" / "0003_mart_daily_sales.sql"


def _normalize(sql: str) -> str:
    """Collapse whitespace and case so generated DDL can be compared to files."""
    return re.sub(r"\s+", " ", sql).strip().lower()


class TestRegistry:
    def test_registry_holds_the_slice_one_mart(self) -> None:
        assert set(MARTS) == {"mart_daily_sales"}
        assert MARTS["mart_daily_sales"] is MART_DAILY_SALES

    def test_mart_by_name_returns_spec(self) -> None:
        assert mart_by_name("mart_daily_sales") is MART_DAILY_SALES

    def test_mart_by_name_unknown_lists_known_marts(self) -> None:
        with pytest.raises(ValueError, match="unknown mart: 'nope'"):
            mart_by_name("nope")


class TestMartSpec:
    def test_table_names(self) -> None:
        assert MART_DAILY_SALES.serving_table == "analytics.mart_daily_sales"
        assert MART_DAILY_SALES.staging_table == "analytics.mart_daily_sales_staging"

    def test_column_names_order(self) -> None:
        assert MART_DAILY_SALES.column_names == (
            "date_key",
            "order_date",
            "category_name",
            "region",
            "orders_count",
            "items_sold",
            "revenue_eur",
            "margin_eur",
        )

    def test_create_table_sql_declares_columns_engine_and_order_by(self) -> None:
        sql = MART_DAILY_SALES.serving_create_sql
        assert sql.startswith("create table if not exists analytics.mart_daily_sales")
        for column in MART_DAILY_SALES.columns:
            assert f"{column.name} {column.clickhouse_type}" in sql
        assert "engine = MergeTree" in sql
        assert "order by (order_date, category_name, region)" in sql

    def test_staging_twin_has_identical_ddl_modulo_table_name(self) -> None:
        serving = MART_DAILY_SALES.serving_create_sql
        staging = MART_DAILY_SALES.staging_create_sql
        assert staging == serving.replace(
            "analytics.mart_daily_sales", "analytics.mart_daily_sales_staging"
        )

    def test_validate_rejects_unknown_order_by_column(self) -> None:
        spec = MartSpec(
            name="broken",
            columns=(MartColumnSpec("a", "Int32"),),
            order_by=("b",),
        )
        with pytest.raises(ValueError, match="order_by references unknown columns"):
            spec.validate()

    def test_validate_rejects_duplicate_columns(self) -> None:
        spec = MartSpec(
            name="broken",
            columns=(MartColumnSpec("a", "Int32"), MartColumnSpec("a", "String")),
            order_by=("a",),
        )
        with pytest.raises(ValueError, match="duplicate column names"):
            spec.validate()


class TestMigrationCrossCheck:
    """The migration file and the generated rebuild DDL must not drift apart."""

    def test_migration_carries_the_generated_serving_and_staging_ddl(self) -> None:
        migration = _normalize(MIGRATION_0003.read_text())
        assert _normalize(MART_DAILY_SALES.serving_create_sql) in migration
        assert _normalize(MART_DAILY_SALES.staging_create_sql) in migration
