"""Unit tests for the declarative snapshot table specs."""

import pyarrow as pa
import pytest

from omni_retail.ingestion.postgres_snapshot.tables import (
    TABLES,
    UPDATED_AT,
    table_by_name,
)

EXPECTED_TABLES = {
    "categories",
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "shipments",
}


def test_registry_contains_exactly_the_oltp_tables() -> None:
    assert set(TABLES) == EXPECTED_TABLES


def test_every_table_has_pk_and_watermark_columns() -> None:
    for spec in TABLES.values():
        names = [column.name for column in spec.columns]
        assert spec.pk in names
        assert UPDATED_AT in names


def test_source_and_batch_prefixes_are_deterministic() -> None:
    orders = TABLES["orders"]
    assert orders.source_name == "postgres-orders"
    assert orders.batch_prefix == "postgres-orders-"


def test_arrow_schema_covers_all_columns_in_order() -> None:
    customers = TABLES["customers"]
    schema = customers.arrow_schema()
    assert schema.names == [column.name for column in customers.columns]
    assert schema.field("customer_id").type == pa.int64()
    assert schema.field("email").type == pa.string()
    assert schema.field("registered_at").type == pa.timestamp("us", tz="UTC")


def test_nullable_columns_are_declared() -> None:
    shipments = TABLES["shipments"]
    schema = shipments.arrow_schema()
    assert schema.field("shipped_at").nullable
    assert schema.field("delivered_at").nullable
    assert not schema.field("created_at").nullable
    categories = TABLES["categories"]
    assert categories.arrow_schema().field("parent_category_id").nullable


def test_column_index_resolves_and_rejects_unknown() -> None:
    orders = TABLES["orders"]
    assert orders.column_index(UPDATED_AT) == len(orders.columns) - 1
    with pytest.raises(ValueError, match="unknown column"):
        orders.column_index("nope")


def test_table_by_name_rejects_unknown_tables() -> None:
    with pytest.raises(ValueError, match="unknown postgres snapshot table"):
        table_by_name("pg_catalog_tables")
