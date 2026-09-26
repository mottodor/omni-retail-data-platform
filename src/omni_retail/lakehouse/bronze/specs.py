"""Declarative specs of the Iceberg Bronze tables (Phase 5 design spec §4).

Bronze stays close to the source representation: business columns mirror the
source types (Trino types for OLTP snapshots; inferred Trino types for the
flattened API envelopes), plus four service columns on every table:

- ``_batch_id``      deterministic logical batch identity (``<source>-<yyyymmdd>``);
- ``_batch_date``    logical date; the table's Iceberg partition column;
- ``_source_object`` archive object key the row was loaded from;
- ``_ingested_at``   wall-clock load time.

Specs follow the pattern of ``ingestion/postgres_snapshot/tables.py``; a unit
test cross-checks the seven OLTP Bronze specs against the snapshot specs so
the two cannot drift apart silently.
"""

import re
from dataclasses import dataclass
from datetime import date
from typing import Literal

SourceKind = Literal["postgres", "api"]

SCHEMA_BRONZE = "bronze"

BATCH_ID = "_batch_id"
BATCH_DATE = "_batch_date"
SOURCE_OBJECT = "_source_object"
INGESTED_AT = "_ingested_at"


@dataclass(frozen=True)
class BronzeColumnSpec:
    """One Bronze column with its explicit Trino type."""

    name: str
    trino_type: str
    nullable: bool = False


@dataclass(frozen=True)
class BronzeTableSpec:
    """Contract of one Bronze table and the raw archive location it loads from."""

    name: str
    kind: SourceKind
    source_name: str
    columns: tuple[BronzeColumnSpec, ...]
    envelope_field: str | None = None  # api only: list field inside the page envelope
    schema_version: str = "1.0"

    @property
    def all_columns(self) -> tuple[BronzeColumnSpec, ...]:
        """Business columns followed by the service columns (DDL/INSERT order)."""
        return self.columns + SERVICE_COLUMNS

    @property
    def source_key(self) -> str:
        """CLI source key: OLTP table name for snapshots, API source name otherwise."""
        return self.name if self.kind == "postgres" else self.source_name

    @property
    def data_object_suffix(self) -> str:
        """Raw object file suffix for this kind (parquet for PG, json for API)."""
        return "parquet" if self.kind == "postgres" else "json"

    def batch_id(self, logical_date: date) -> str:
        """Deterministic logical batch id: ``<source_name>-<yyyymmdd>``."""
        return f"{self.source_name}-{logical_date:%Y%m%d}"

    @property
    def root_prefix(self) -> str:
        """Archive-bucket prefix holding every raw object of the source (all dates)."""
        if self.kind == "postgres":
            return f"postgres/{self.name}/"
        return f"api/{self.source_name}/"

    def logical_date_from_key(self, key: str) -> date | None:
        """Parse the logical date of a raw data-object key under :attr:`root_prefix`.

        Returns ``None`` for keys that do not match the source layout or carry
        an invalid date (e.g., foreign objects dropped under the prefix): such
        keys simply do not address a loadable logical date.
        """
        prefix = self.root_prefix
        relative = key[len(prefix) :] if key.startswith(prefix) else key
        if self.kind == "postgres":
            match = re.fullmatch(r"(\d{4})/(\d{2})/(\d{2})/.+", relative)
            if match is None:
                return None
            year, month, day = (int(part) for part in match.groups())
        else:
            match = re.fullmatch(r"(\d{8})/.+", relative)
            if match is None:
                return None
            stamped = int(match.group(1))
            year, month, day = stamped // 10000, stamped // 100 % 100, stamped % 100
        try:
            return date(year, month, day)
        except ValueError:
            return None

    def object_prefix(self, logical_date: date) -> str:
        """Archive-bucket prefix holding the raw objects of one logical date."""
        if self.kind == "postgres":
            return f"postgres/{self.name}/{logical_date:%Y/%m/%d}/"
        return f"api/{self.source_name}/{logical_date:%Y%m%d}/"

    def validate(self) -> None:
        names = [column.name for column in self.columns]
        if not names:
            raise ValueError(f"{self.name}: no business columns declared")
        if len(names) != len(set(names)):
            raise ValueError(f"{self.name}: duplicate column names")
        if {column.name for column in SERVICE_COLUMNS}.intersection(names):
            raise ValueError(f"{self.name}: service column names are reserved")
        if self.kind == "api" and not self.envelope_field:
            raise ValueError(f"{self.name}: api specs must declare envelope_field")
        if self.kind == "postgres" and self.envelope_field:
            raise ValueError(f"{self.name}: postgres specs must not declare envelope_field")


def _bigint(name: str, *, nullable: bool = False) -> BronzeColumnSpec:
    return BronzeColumnSpec(name, "bigint", nullable)


def _varchar(name: str, *, nullable: bool = False) -> BronzeColumnSpec:
    return BronzeColumnSpec(name, "varchar", nullable)


def _boolean(name: str) -> BronzeColumnSpec:
    return BronzeColumnSpec(name, "boolean")


def _decimal(name: str, precision: int = 12, scale: int = 2) -> BronzeColumnSpec:
    return BronzeColumnSpec(name, f"decimal({precision},{scale})")


def _tstz(name: str, *, nullable: bool = False) -> BronzeColumnSpec:
    return BronzeColumnSpec(name, "timestamp(6) with time zone", nullable)


def _date(name: str, *, nullable: bool = False) -> BronzeColumnSpec:
    return BronzeColumnSpec(name, "date", nullable)


SERVICE_COLUMNS: tuple[BronzeColumnSpec, ...] = (
    BronzeColumnSpec(BATCH_ID, "varchar"),
    BronzeColumnSpec(BATCH_DATE, "date"),
    BronzeColumnSpec(SOURCE_OBJECT, "varchar"),
    BronzeColumnSpec(INGESTED_AT, "timestamp(6) with time zone"),
)

CATEGORIES = BronzeTableSpec(
    name="categories",
    kind="postgres",
    source_name="postgres-categories",
    columns=(
        _bigint("category_id"),
        _varchar("name"),
        _bigint("parent_category_id", nullable=True),
        _tstz("created_at"),
        _tstz("updated_at"),
    ),
)

PRODUCTS = BronzeTableSpec(
    name="products",
    kind="postgres",
    source_name="postgres-products",
    columns=(
        _bigint("product_id"),
        _varchar("sku"),
        _varchar("name"),
        _bigint("category_id"),
        _varchar("brand"),
        _decimal("unit_price"),
        _decimal("unit_cost"),
        _boolean("is_active"),
        _tstz("created_at"),
        _tstz("updated_at"),
    ),
)

CUSTOMERS = BronzeTableSpec(
    name="customers",
    kind="postgres",
    source_name="postgres-customers",
    columns=(
        _bigint("customer_id"),
        _varchar("email"),
        _varchar("first_name"),
        _varchar("last_name"),
        _varchar("region"),
        _varchar("city"),
        _varchar("status"),
        _varchar("segment"),
        _tstz("registered_at"),
        _tstz("created_at"),
        _tstz("updated_at"),
    ),
)

ORDERS = BronzeTableSpec(
    name="orders",
    kind="postgres",
    source_name="postgres-orders",
    columns=(
        _bigint("order_id"),
        _bigint("customer_id"),
        _varchar("status"),
        _varchar("currency"),
        _decimal("shipping_cost"),
        _decimal("order_total"),
        _tstz("created_at"),
        _tstz("updated_at"),
    ),
)

ORDER_ITEMS = BronzeTableSpec(
    name="order_items",
    kind="postgres",
    source_name="postgres-order_items",
    columns=(
        _bigint("order_item_id"),
        _bigint("order_id"),
        _bigint("product_id"),
        _bigint("quantity"),
        _decimal("unit_price"),
        _decimal("line_total"),
        _tstz("created_at"),
        _tstz("updated_at"),
    ),
)

PAYMENTS = BronzeTableSpec(
    name="payments",
    kind="postgres",
    source_name="postgres-payments",
    columns=(
        _bigint("payment_id"),
        _bigint("order_id"),
        _varchar("method"),
        _varchar("status"),
        _decimal("amount"),
        _varchar("transaction_id"),
        _tstz("created_at"),
        _tstz("updated_at"),
    ),
)

SHIPMENTS = BronzeTableSpec(
    name="shipments",
    kind="postgres",
    source_name="postgres-shipments",
    columns=(
        _bigint("shipment_id"),
        _bigint("order_id"),
        _varchar("carrier"),
        _varchar("tracking_number"),
        _varchar("status"),
        _tstz("shipped_at", nullable=True),
        _tstz("delivered_at", nullable=True),
        _tstz("created_at"),
        _tstz("updated_at"),
    ),
)

FX_RATES = BronzeTableSpec(
    name="fx_rates",
    kind="api",
    source_name="fx-rates",
    envelope_field="rates",
    columns=(
        _varchar("currency"),
        _decimal("rate", 18, 6),
    ),
)

CAMPAIGNS = BronzeTableSpec(
    name="campaigns",
    kind="api",
    source_name="marketing-campaigns",
    envelope_field="campaigns",
    columns=(
        _varchar("campaign_id"),
        _varchar("name"),
        _varchar("channel"),
        _varchar("status"),
        _date("start_date"),
        _date("end_date"),
        _decimal("budget_eur"),
        _decimal("spend_eur"),
        _bigint("impressions"),
        _bigint("clicks"),
    ),
)

DELIVERIES = BronzeTableSpec(
    name="deliveries",
    kind="api",
    source_name="deliveries",
    envelope_field="deliveries",
    columns=(
        _varchar("delivery_id"),
        _varchar("order_id"),
        _varchar("carrier"),
        _varchar("status"),
        _tstz("shipped_at"),
        _tstz("delivered_at", nullable=True),
        _tstz("updated_at"),
    ),
)

TABLES: dict[str, BronzeTableSpec] = {
    spec.name: spec
    for spec in (
        CATEGORIES,
        PRODUCTS,
        CUSTOMERS,
        ORDERS,
        ORDER_ITEMS,
        PAYMENTS,
        SHIPMENTS,
        FX_RATES,
        CAMPAIGNS,
        DELIVERIES,
    )
}

for _spec in TABLES.values():
    _spec.validate()


def spec_by_source(source: str) -> BronzeTableSpec:
    """Return the spec registered under a CLI source key."""
    for spec in TABLES.values():
        if spec.source_key == source:
            return spec
    known = ", ".join(sorted(spec.source_key for spec in TABLES.values()))
    raise ValueError(f"unknown bronze source: {source!r} (known: {known})")


def all_sources() -> tuple[str, ...]:
    """CLI source keys in registration order (used by ``run-all``)."""
    return tuple(spec.source_key for spec in TABLES.values())
