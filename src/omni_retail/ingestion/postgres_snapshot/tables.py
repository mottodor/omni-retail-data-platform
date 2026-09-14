"""Declarative specs of the snapshot OLTP tables (Phase 4 design spec §5).

Columns and pyarrow types mirror the Phase 2 OLTP schema
(``postgres/init/01_oltp_schema.sql``): explicit schemas keep serialization
deterministic and testable, like ``files/schemas.py`` for file sources.
"""

from dataclasses import dataclass

import pyarrow as pa
from pyarrow import DataType

#: Updated-at watermark column; every snapshot table must have it.
UPDATED_AT = "updated_at"


@dataclass(frozen=True)
class ColumnSpec:
    """One extracted column with its explicit Arrow type."""

    name: str
    type: DataType
    nullable: bool = False


@dataclass(frozen=True)
class TableSpec:
    """Contract of one snapshot-extracted table."""

    name: str
    pk: str
    columns: tuple[ColumnSpec, ...]
    schema_version: str = "1.0"

    @property
    def source_name(self) -> str:
        """Registry source name; also the prefix of ``batch_id``."""
        return f"postgres-{self.name}"

    @property
    def batch_prefix(self) -> str:
        return f"postgres-{self.name}-"

    def validate(self) -> None:
        names = [column.name for column in self.columns]
        if self.pk not in names:
            raise ValueError(f"{self.name}: primary key {self.pk!r} missing from columns")
        if UPDATED_AT not in names:
            raise ValueError(f"{self.name}: watermark column {UPDATED_AT!r} missing")
        if len(names) != len(set(names)):
            raise ValueError(f"{self.name}: duplicate column names")

    def arrow_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field(column.name, column.type, nullable=column.nullable)
                for column in self.columns
            ]
        )

    def column_index(self, name: str) -> int:
        """Position of a column inside extracted row tuples (SELECT order)."""
        for index, column in enumerate(self.columns):
            if column.name == name:
                return index
        raise ValueError(f"{self.name}: unknown column {name!r}")


def _text(name: str, *, nullable: bool = False) -> ColumnSpec:
    return ColumnSpec(name, pa.string(), nullable)


def _int64(name: str, *, nullable: bool = False) -> ColumnSpec:
    return ColumnSpec(name, pa.int64(), nullable)


def _bool(name: str) -> ColumnSpec:
    return ColumnSpec(name, pa.bool_())


def _decimal(name: str) -> ColumnSpec:
    return ColumnSpec(name, pa.decimal128(12, 2))


def _timestamp(name: str, *, nullable: bool = False) -> ColumnSpec:
    return ColumnSpec(name, pa.timestamp("us", tz="UTC"), nullable)


CATEGORIES = TableSpec(
    name="categories",
    pk="category_id",
    columns=(
        _int64("category_id"),
        _text("name"),
        _int64("parent_category_id", nullable=True),
        _timestamp("created_at"),
        _timestamp(UPDATED_AT),
    ),
)

PRODUCTS = TableSpec(
    name="products",
    pk="product_id",
    columns=(
        _int64("product_id"),
        _text("sku"),
        _text("name"),
        _int64("category_id"),
        _text("brand"),
        _decimal("unit_price"),
        _decimal("unit_cost"),
        _bool("is_active"),
        _timestamp("created_at"),
        _timestamp(UPDATED_AT),
    ),
)

CUSTOMERS = TableSpec(
    name="customers",
    pk="customer_id",
    columns=(
        _int64("customer_id"),
        _text("email"),
        _text("first_name"),
        _text("last_name"),
        _text("region"),
        _text("city"),
        _text("status"),
        _text("segment"),
        _timestamp("registered_at"),
        _timestamp("created_at"),
        _timestamp(UPDATED_AT),
    ),
)

ORDERS = TableSpec(
    name="orders",
    pk="order_id",
    columns=(
        _int64("order_id"),
        _int64("customer_id"),
        _text("status"),
        _text("currency"),
        _decimal("shipping_cost"),
        _decimal("order_total"),
        _timestamp("created_at"),
        _timestamp(UPDATED_AT),
    ),
)

ORDER_ITEMS = TableSpec(
    name="order_items",
    pk="order_item_id",
    columns=(
        _int64("order_item_id"),
        _int64("order_id"),
        _int64("product_id"),
        _int64("quantity"),
        _decimal("unit_price"),
        _decimal("line_total"),
        _timestamp("created_at"),
        _timestamp(UPDATED_AT),
    ),
)

PAYMENTS = TableSpec(
    name="payments",
    pk="payment_id",
    columns=(
        _int64("payment_id"),
        _int64("order_id"),
        _text("method"),
        _text("status"),
        _decimal("amount"),
        _text("transaction_id"),
        _timestamp("created_at"),
        _timestamp(UPDATED_AT),
    ),
)

SHIPMENTS = TableSpec(
    name="shipments",
    pk="shipment_id",
    columns=(
        _int64("shipment_id"),
        _int64("order_id"),
        _text("carrier"),
        _text("tracking_number"),
        _text("status"),
        _timestamp("shipped_at", nullable=True),
        _timestamp("delivered_at", nullable=True),
        _timestamp("created_at"),
        _timestamp(UPDATED_AT),
    ),
)

TABLES: dict[str, TableSpec] = {
    spec.name: spec
    for spec in (CATEGORIES, PRODUCTS, CUSTOMERS, ORDERS, ORDER_ITEMS, PAYMENTS, SHIPMENTS)
}

for _spec in TABLES.values():
    _spec.validate()


def table_by_name(name: str) -> TableSpec:
    """Return the table spec registered under ``name``."""
    try:
        return TABLES[name]
    except KeyError as error:
        known = ", ".join(sorted(TABLES))
        raise ValueError(f"unknown postgres snapshot table: {name!r} (known: {known})") from error
