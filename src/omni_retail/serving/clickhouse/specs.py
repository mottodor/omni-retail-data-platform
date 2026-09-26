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
        return (
            f"create table if not exists {table}\n(\n    {columns}\n)\n"
            f"engine = {self.engine}\norder by ({order_by})"
        )

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
)
# Physical design note (guide §26.2, ADR 0004): MergeTree with the mart
# grain as ORDER BY is the interim choice — the per-mart ORDER BY /
# partitioning / TTL design is deliberately re-evaluated in Phase 6
# slice 2 via a new migration (serving data is derived; republish only).

MARTS: dict[str, MartSpec] = {spec.name: spec for spec in (MART_DAILY_SALES,)}

for _spec in MARTS.values():
    _spec.validate()


def mart_by_name(name: str) -> MartSpec:
    """Return the spec registered under a CLI mart name."""
    try:
        return MARTS[name]
    except KeyError:
        known = ", ".join(sorted(MARTS))
        raise ValueError(f"unknown mart: {name!r} (known: {known})") from None
