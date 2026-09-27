"""Unit tests for the serving mart specs and the migration cross-check."""

import re
from pathlib import Path

import pytest

from omni_retail.serving.clickhouse.specs import (
    MART_CUSTOMER_LTV,
    MART_DAILY_SALES,
    MART_DELIVERY_PERFORMANCE,
    MART_MARKETING_ROI,
    MARTS,
    MartColumnSpec,
    MartSpec,
    mart_by_name,
)

REPO_ROOT = Path(__file__).resolve().parents[4]
MIGRATION_DIR = REPO_ROOT / "clickhouse" / "migrations"

#: Migration file that carries the CURRENT DDL of each mart. For
#: mart_daily_sales that is 0007 (the slice-2 re-partitioning recreation);
#: 0003 remains in the ledger as history only.
DDL_CARRIERS = {
    "mart_daily_sales": "0007_mart_daily_sales_monthly_partition.sql",
    "mart_customer_ltv": "0004_mart_customer_ltv.sql",
    "mart_marketing_roi": "0005_mart_marketing_roi.sql",
    "mart_delivery_performance": "0006_mart_delivery_performance.sql",
}


def _normalize(sql: str) -> str:
    """Collapse whitespace and case so generated DDL can be compared to files."""
    return re.sub(r"\s+", " ", sql).strip().lower()


class TestRegistry:
    def test_registry_holds_every_registered_mart(self) -> None:
        assert set(MARTS) == {
            "mart_daily_sales",
            "mart_customer_ltv",
            "mart_marketing_roi",
            "mart_delivery_performance",
        }
        assert MARTS["mart_daily_sales"] is MART_DAILY_SALES
        assert MARTS["mart_customer_ltv"] is MART_CUSTOMER_LTV
        assert MARTS["mart_marketing_roi"] is MART_MARKETING_ROI
        assert MARTS["mart_delivery_performance"] is MART_DELIVERY_PERFORMANCE

    def test_registry_order_is_stable(self) -> None:
        # rebuild --all (and its logs) follows registry order; dict literals
        # preserve insertion order, asserted here so reordering is conscious.
        assert list(MARTS) == [
            "mart_daily_sales",
            "mart_customer_ltv",
            "mart_marketing_roi",
            "mart_delivery_performance",
        ]

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

    def test_partitioned_mart_emits_partition_by(self) -> None:
        sql = MART_DAILY_SALES.serving_create_sql
        assert "partition by toYYYYMM(order_date)" in sql
        assert sql.index("order by") < sql.index("partition by")

    @pytest.mark.parametrize(
        "spec", [MART_CUSTOMER_LTV, MART_MARKETING_ROI, MART_DELIVERY_PERFORMANCE]
    )
    def test_unpartitioned_marts_emit_no_partition_clause(self, spec: MartSpec) -> None:
        assert "partition by" not in spec.serving_create_sql

    def test_single_column_order_by(self) -> None:
        assert "order by (carrier)" in MART_DELIVERY_PERFORMANCE.serving_create_sql

    def test_nullable_columns_only_where_the_model_can_produce_nulls(self) -> None:
        nullable = {
            spec.name: {c.name for c in spec.columns if c.clickhouse_type.startswith("Nullable")}
            for spec in MARTS.values()
        }
        assert nullable == {
            "mart_daily_sales": set(),
            "mart_customer_ltv": {"avg_order_value_eur"},
            "mart_marketing_roi": {"end_date", "ctr", "cpc_eur", "cpm_eur", "budget_utilization"},
            "mart_delivery_performance": {"avg_transit_hours"},
        }

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
    """The current-DDL migration file and the generated rebuild DDL must not drift."""

    @pytest.mark.parametrize("mart_name", sorted(DDL_CARRIERS))
    def test_migration_carries_the_generated_serving_and_staging_ddl(self, mart_name: str) -> None:
        migration = _normalize((MIGRATION_DIR / DDL_CARRIERS[mart_name]).read_text())
        spec = MARTS[mart_name]
        assert _normalize(spec.serving_create_sql) in migration
        assert _normalize(spec.staging_create_sql) in migration
