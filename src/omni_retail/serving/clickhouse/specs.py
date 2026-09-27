"""Declarative specs of the ClickHouse serving marts (Phase 6, ADR 0004).

Every serving table mirrors its Gold mart 1:1 — same columns in the same
order, no business logic on the ClickHouse side (AGENTS §3.2): ClickHouse
is a derived, rebuildable copy of Iceberg Gold. The generated DDL is the
single source used by ``rebuild``; the paired migration under
``clickhouse/migrations/`` must carry the identical statements — a unit
test cross-checks the two so they cannot drift apart silently (the same
pattern the Bronze specs use against the snapshot specs).
"""

from dataclasses import dataclass

SCHEMA_SOURCE = "analytics"  # Trino-side schema of the Gold marts
SCHEMA_SERVING = "analytics"  # ClickHouse-side serving database
STAGING_SUFFIX = "_staging"


@dataclass(frozen=True)
class MartColumnSpec:
    """One serving column: the Gold column name plus its ClickHouse type."""

    name: str
    clickhouse_type: str


@dataclass(frozen=True)
class MartSpec:
    """Contract of one serving mart: source, targets, columns, physical design."""

    name: str
    columns: tuple[MartColumnSpec, ...]
    order_by: tuple[str, ...]
    engine: str = "MergeTree"
    partition_by: str | None = None
    source_schema: str = SCHEMA_SOURCE
    serving_schema: str = SCHEMA_SERVING

    @property
    def serving_table(self) -> str:
        """Fully qualified serving table (ClickHouse side)."""
        return f"{self.serving_schema}.{self.name}"

    @property
    def staging_table(self) -> str:
        """Fully qualified staging twin; identical DDL, required for the swap."""
        return f"{self.serving_schema}.{self.name}{STAGING_SUFFIX}"

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    def create_table_sql(self, table: str) -> str:
        columns = ",\n    ".join(f"{c.name} {c.clickhouse_type}" for c in self.columns)
        order_by = ", ".join(self.order_by)
        sql = (
            f"create table if not exists {table}\n(\n    {columns}\n)\n"
            f"engine = {self.engine}\norder by ({order_by})"
        )
        if self.partition_by is not None:
            sql += f"\npartition by {self.partition_by}"
        return sql

    @property
    def serving_create_sql(self) -> str:
        return self.create_table_sql(self.serving_table)

    @property
    def staging_create_sql(self) -> str:
        return self.create_table_sql(self.staging_table)

    def validate(self) -> None:
        names = self.column_names
        if not names:
            raise ValueError(f"{self.name}: no columns declared")
        if len(names) != len(set(names)):
            raise ValueError(f"{self.name}: duplicate column names")
        unknown = set(self.order_by) - set(names)
        if unknown:
            raise ValueError(f"{self.name}: order_by references unknown columns: {sorted(unknown)}")


MART_DAILY_SALES = MartSpec(
    name="mart_daily_sales",
    columns=(
        MartColumnSpec("date_key", "Int32"),  # Trino integer (yyyymmdd key)
        MartColumnSpec("order_date", "Date"),  # Trino date
        MartColumnSpec("category_name", "String"),  # Trino varchar
        MartColumnSpec("region", "String"),
        MartColumnSpec("orders_count", "UInt64"),  # Trino bigint counts
        MartColumnSpec("items_sold", "UInt64"),
        # Money mirrors the Gold types exactly (decimal division artifacts
        # included) so values round-trip 1:1; normalization is a slice 2 call.
        MartColumnSpec("revenue_eur", "Decimal(38, 21)"),  # Trino decimal(38,21)
        MartColumnSpec("margin_eur", "Decimal(38, 6)"),  # Trino decimal(38,6)
    ),
    order_by=("order_date", "category_name", "region"),
    partition_by="toYYYYMM(order_date)",
)
# Physical design (guide §26.2, ratified in slice 2 — ADR 0004 deferral):
# MergeTree for every mart — the swap-based publication makes dedup engines
# unnecessary. The only partitioned mart is the one with a natural date
# filter/grain: monthly toYYYYMM(order_date) keeps the slice-3
# REPLACE PARTITION option open for mart_daily_sales; the other three are
# small full-history snapshots — unpartitioned. No TTL anywhere: historical
# portfolio data, expiry has no meaning.

MART_CUSTOMER_LTV = MartSpec(
    name="mart_customer_ltv",
    columns=(
        MartColumnSpec("customer_id", "Int64"),  # Trino bigint (OLTP identifier)
        MartColumnSpec("customer_key", "String"),  # Trino varchar (SCD2 surrogate)
        MartColumnSpec("region", "String"),
        MartColumnSpec("segment", "String"),
        MartColumnSpec("first_order_date", "Date"),  # Trino date
        MartColumnSpec("last_order_date", "Date"),
        MartColumnSpec("orders_count", "UInt64"),  # Trino bigint count
        # Money mirrors the Gold types exactly (decimal(38,21) division
        # artifacts included): the exact round-trip keeps reconciliation
        # trivially provable — normalizing scales is deliberately rejected.
        MartColumnSpec("gmv_eur", "Decimal(38, 21)"),
        # case-without-else: NULL when the customer has no realized orders.
        MartColumnSpec("avg_order_value_eur", "Nullable(Decimal(38, 21))"),
    ),
    # LTV panels filter by region/segment, then locate customers; the grain
    # column closes the key. Full-history snapshot: no partitioning, no TTL.
    order_by=("region", "segment", "customer_id"),
)

MART_MARKETING_ROI = MartSpec(
    name="mart_marketing_roi",
    columns=(
        MartColumnSpec("campaign_id", "String"),
        MartColumnSpec("campaign_name", "String"),
        MartColumnSpec("channel", "String"),
        MartColumnSpec("campaign_status", "String"),
        MartColumnSpec("start_date", "Date"),
        MartColumnSpec("end_date", "Nullable(Date)"),  # ongoing campaigns
        MartColumnSpec("budget_eur", "Decimal(12, 2)"),  # Trino decimal(12,2)
        MartColumnSpec("spend_eur", "Decimal(12, 2)"),
        MartColumnSpec("impressions", "UInt64"),  # Trino bigint counts
        MartColumnSpec("clicks", "UInt64"),
        MartColumnSpec("ctr", "Nullable(Float64)"),  # nullif(impressions, 0)
        MartColumnSpec("cpc_eur", "Nullable(Float64)"),  # nullif(clicks, 0)
        MartColumnSpec("cpm_eur", "Nullable(Float64)"),  # nullif(impressions, 0)
        MartColumnSpec("budget_utilization", "Nullable(Decimal(27, 15))"),
    ),
    # Campaign panels filter by channel and start_date range; campaign_id
    # ties the key to the grain. Hundreds of rows: no partitioning, no TTL.
    order_by=("channel", "start_date", "campaign_id"),
)

MART_DELIVERY_PERFORMANCE = MartSpec(
    name="mart_delivery_performance",
    columns=(
        MartColumnSpec("carrier", "String"),
        MartColumnSpec("delivery_count", "UInt64"),
        MartColumnSpec("delivered_count", "UInt64"),
        MartColumnSpec("in_transit_count", "UInt64"),
        MartColumnSpec("delayed_count", "UInt64"),
        MartColumnSpec("returned_count", "UInt64"),
        # case-without-else: NULL until the carrier has a completed delivery.
        MartColumnSpec("avg_transit_hours", "Nullable(Float64)"),
        # Trino timestamp(6) with time zone; explicit UTC so reads are
        # deterministic regardless of the server timezone.
        MartColumnSpec("last_update_at", "DateTime64(6, 'UTC')"),
    ),
    # One row per carrier: carrier is the only access key. Dozens of rows:
    # no partitioning, no TTL.
    order_by=("carrier",),
)

#: Nullability rule: Nullable(..) only where the Gold model can produce
#: NULLs (nullif divisions, case-without-else, optional source attributes);
#: everything else is NOT NULL by construction — an unexpected NULL fails
#: the staging insert loudly instead of corrupting the serving copy.

MARTS: dict[str, MartSpec] = {
    spec.name: spec
    for spec in (
        MART_DAILY_SALES,
        MART_CUSTOMER_LTV,
        MART_MARKETING_ROI,
        MART_DELIVERY_PERFORMANCE,
    )
}

for _spec in MARTS.values():
    _spec.validate()


def mart_by_name(name: str) -> MartSpec:
    """Return the spec registered under a CLI mart name."""
    try:
        return MARTS[name]
    except KeyError:
        known = ", ".join(sorted(MARTS))
        raise ValueError(f"unknown mart: {name!r} (known: {known})") from None
