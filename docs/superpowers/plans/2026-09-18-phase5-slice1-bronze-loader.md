# Phase 5 / Slice 1 — Bronze Loader + dbt Skeleton Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Load raw archive objects (PG-snapshot Parquet + API JSON pages) from MinIO into Iceberg `bronze` tables via Trino, and add the dbt project skeleton (profiles + 7 OLTP staging models) with offline `dbt parse` in CI.

**Architecture:** A new Python package `src/omni_retail/lakehouse/bronze/` follows the Phase 3/4 ingestion patterns: declarative specs → readers (schema-drift errors) → SQL literal batching → an idempotent loader (`DELETE` partition + batched `INSERT`, row-count verified against the raw manifest) behind a `TrinoExecutor` protocol (fake in unit tests, `trino.dbapi` in prod/integration). The dbt skeleton lives in top-level `dbt/` with an env-driven committed `profiles.yml` (no secrets) and pass-through staging views over `bronze` sources.

**Tech Stack:** Python 3.12 + uv; `dbt-core>=1.10,<1.11` + `dbt-trino>=1.9,<1.10` (new **main** deps; the `trino` client arrives transitively via dbt-trino and is reused by the loader — per spec §7); pyarrow (existing); pytest + fakes.

**Spec:** `docs/superpowers/specs/2026-09-18-phase5-dbt-lakehouse-design.md` (§4 Bronze, §7 deps, §8 config, §9 testing, §10 slice 1). The spec travels with this plan; executors read both.

## Global Constraints

- Python 3.12; uv only (`uv add` / `uv sync`, never pip); `uv.lock` committed.
- ruff rules `E,F,I,UP,B,SIM`, line length 100; `mypy --strict` over `omni_retail` (new code fully typed; `trino.*` gets an `ignore_missing_imports` override).
- No secrets committed; `dbt/profiles.yml` is env-driven only; `.env.example` gets placeholders.
- Idempotency: `load(source, date)` = `DELETE FROM bronze.<t> WHERE _batch_date = date` → batched `INSERT` (~500 rows/statement). No duplicate rows on re-run.
- Slice 1 scope ONLY: no intermediate/core/marts models, no API staging models in dbt (slice 2), no Airflow changes (slice 3), no bronze tables for the 4 file sources (follow-up issue).
- Naming: bronze schema `bronze`; service columns `_batch_id`, `_batch_date` (partition), `_source_object`, `_ingested_at`; SQL identifiers quoted lowercase.
- Raw layout contracts (must match, not reinvent): PG objects `archive/postgres/<table>/<yyyy>/<mm>/<dd>/data.parquet` with manifest `_manifests/postgres-<table>/postgres-<table>-<yyyymmdd>.json`; API pages `archive/api/<source>/<yyyymmdd>/page_XXXX.json` with manifest `_manifests/<source>/<source>-<yyyymmdd>.json`.
- Errors are explicit: `BronzeReadError` (schema drift / bad envelope), `LoadError` (manifest missing / row-count mismatch). A verification failure must NOT execute any DML against the partition.
- Empty batch (no raw objects for the date) = no-op with a warning, exit code 0.
- Conventional Commits (`feat(lakehouse):`, `feat(dbt):`, `test(integration):`, `docs:`).

---

### Task 1: Dependencies and Trino environment config

**Files:**
- Modify: `pyproject.toml` (dependencies + mypy override)
- Modify: `.env.example`
- Generate: `uv.lock`

**Interfaces:**
- Produces: main deps `dbt-core>=1.10,<1.11`, `dbt-trino>=1.9,<1.10` in the venv (and `trino` transitively); env vars `TRINO_HOST` (default `127.0.0.1`), `TRINO_PORT` (`8080`), `TRINO_CATALOG` (`iceberg`), `TRINO_USER` (`omni`).

- [ ] **Step 1: Add pinned dependencies**

```bash
uv add "dbt-core>=1.10,<1.11" "dbt-trino>=1.9,<1.10"
uv sync
```

Verify resolution succeeded and versions land as dbt-core 1.10.x + dbt-trino 1.9.x (dbt-trino 1.9.3 requires `dbt-core>=1.8.0`, so no conflict). If uv cannot resolve this exact pair, widen dbt-trino to `>=1.9,<1.11` and record the deviation in the task commit message.

- [ ] **Step 2: Add mypy override for the untyped trino client**

In `pyproject.toml`, extend the existing override (single `[[tool.mypy.overrides]]` block):

```toml
[[tool.mypy.overrides]]
module = ["pyarrow.*", "openpyxl.*", "trino.*"]
ignore_missing_imports = true
```

- [ ] **Step 3: Document Trino variables in `.env.example`**

Append (after the PostgreSQL OLTP block):

```
# Trino (analytical SQL engine over Iceberg; used from Phase 5). The Bronze
# loader and dbt connect from the host by default; inside the Docker network
# (Airflow, Phase 5 slice 3) TRINO_HOST must be set to `trino`.
TRINO_HOST=127.0.0.1
TRINO_PORT=8080
TRINO_CATALOG=iceberg
TRINO_USER=omni
```

- [ ] **Step 4: Validate**

```bash
uv run python -c "import dbt.version, trino; print(dbt.version.__version__)"
uv run ruff check . && uv run ruff format --check .
uv run mypy src tests
```

Expected: version printed; lint/mypy pass.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml uv.lock .env.example
git commit -m "feat(deps): add pinned dbt-core/dbt-trino and Trino env config (Phase 5 slice 1)"
```

---

### Task 2: Declarative Bronze table specs

**Files:**
- Create: `src/omni_retail/lakehouse/__init__.py` (empty)
- Create: `src/omni_retail/lakehouse/bronze/__init__.py` (empty)
- Create: `src/omni_retail/lakehouse/bronze/specs.py`
- Test: `tests/unit/lakehouse/bronze/test_specs.py`

**Interfaces:**
- Produces: `BronzeColumnSpec(name, trino_type, nullable=False)`, `BronzeTableSpec(name, kind, source_name, columns, envelope_field=None, schema_version="1.0")` with methods `all_columns`, `source_key`, `data_object_suffix`, `batch_id(logical_date)`, `object_prefix(logical_date)`, `validate()`; `SERVICE_COLUMNS`; `TABLES: dict[str, BronzeTableSpec]` (10 entries); `spec_by_source(source)`, `all_sources()`; constants `SCHEMA_BRONZE = "bronze"`, `BATCH_ID`, `BATCH_DATE`, `SOURCE_OBJECT`, `INGESTED_AT`; `SourceKind = Literal["postgres", "api"]`.

- [ ] **Step 1: Write the failing tests** (`tests/unit/lakehouse/bronze/test_specs.py`)

```python
"""Unit tests for the declarative Bronze table specs."""

from datetime import date

import pytest

from omni_retail.ingestion.postgres_snapshot.tables import TABLES as SNAPSHOT_TABLES
from omni_retail.lakehouse.bronze.specs import (
    SERVICE_COLUMNS,
    TABLES,
    all_sources,
    spec_by_source,
)

LOGICAL_DATE = date(2026, 9, 18)


def test_all_ten_tables_registered() -> None:
    assert set(TABLES) == {
        "orders",
        "order_items",
        "customers",
        "products",
        "categories",
        "payments",
        "shipments",
        "fx_rates",
        "campaigns",
        "deliveries",
    }


def test_oltp_specs_mirror_snapshot_column_names() -> None:
    for name, snapshot in SNAPSHOT_TABLES.items():
        bronze = TABLES[name]
        assert [column.name for column in bronze.columns] == [
            column.name for column in snapshot.columns
        ], f"bronze spec for {name} drifted from the snapshot spec"


def test_batch_ids_match_raw_producers() -> None:
    assert TABLES["orders"].batch_id(LOGICAL_DATE) == "postgres-orders-20260918"
    assert TABLES["fx_rates"].batch_id(LOGICAL_DATE) == "fx-rates-20260918"
    assert TABLES["campaigns"].batch_id(LOGICAL_DATE) == "marketing-campaigns-20260918"


def test_object_prefixes_match_raw_layout() -> None:
    assert TABLES["orders"].object_prefix(LOGICAL_DATE) == "postgres/orders/2026/09/18/"
    assert TABLES["fx_rates"].object_prefix(LOGICAL_DATE) == "api/fx-rates/20260918/"


def test_service_columns_are_last_and_partition_on_batch_date() -> None:
    assert [column.name for column in SERVICE_COLUMNS] == [
        "_batch_id",
        "_batch_date",
        "_source_object",
        "_ingested_at",
    ]
    assert SERVICE_COLUMNS[1].trino_type == "date"
    assert TABLES["orders"].all_columns[-4:] == SERVICE_COLUMNS


def test_api_specs_declare_envelope_fields() -> None:
    assert TABLES["fx_rates"].envelope_field == "rates"
    assert TABLES["campaigns"].envelope_field == "campaigns"
    assert TABLES["deliveries"].envelope_field == "deliveries"
    assert TABLES["orders"].envelope_field is None


def test_spec_by_source_resolves_cli_keys() -> None:
    assert spec_by_source("orders").name == "orders"
    assert spec_by_source("fx-rates").name == "fx_rates"
    assert spec_by_source("marketing-campaigns").name == "campaigns"
    assert spec_by_source("deliveries").name == "deliveries"
    assert len(all_sources()) == 10


def test_spec_by_source_rejects_unknown() -> None:
    with pytest.raises(ValueError, match="unknown bronze source"):
        spec_by_source("nope")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/lakehouse/bronze/test_specs.py -v`
Expected: FAIL with `ModuleNotFoundError` (no `omni_retail.lakehouse`).

- [ ] **Step 3: Implement `specs.py`**

```python
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
```

Note: `source_name` for `order_items` is `postgres-order_items` — this matches `TableSpec.source_name` from `ingestion/postgres_snapshot/tables.py` (`f"postgres-{self.name}"` where name is `order_items`); add an assertion in Step 1's test file is already covered by `test_batch_ids_match_raw_producers` style — additionally verify `test_oltp_specs_mirror_snapshot_column_names` passes and extend it with:

```python
def test_oltp_source_names_match_snapshot_registry() -> None:
    for name, snapshot in SNAPSHOT_TABLES.items():
        assert TABLES[name].source_name == snapshot.source_name
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/lakehouse/bronze/test_specs.py -v`
Expected: PASS (9 tests).

- [ ] **Step 5: Commit**

```bash
git add src/omni_retail/lakehouse tests/unit/lakehouse
git commit -m "feat(lakehouse): declarative Bronze table specs (Phase 5 slice 1)"
```

---

### Task 3: Trino SQL literals and batched INSERTs

**Files:**
- Create: `src/omni_retail/lakehouse/bronze/literals.py`
- Test: `tests/unit/lakehouse/bronze/test_literals.py`

**Interfaces:**
- Consumes: `BronzeTableSpec` from Task 2.
- Produces: `ROWS_PER_STATEMENT = 500`; `Row = Mapping[str, object]`; `varchar_literal(value) -> str`; `timestamp_literal(value) -> str`; `date_literal(value) -> str`; `sql_literal(value, trino_type) -> str` (raises `TypeError` on type mismatch); `insert_statements(spec, rows, *, catalog="iceberg", schema="bronze", rows_per_statement=ROWS_PER_STATEMENT) -> list[str]`.

- [ ] **Step 1: Write the failing tests** (`tests/unit/lakehouse/bronze/test_literals.py`)

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/lakehouse/bronze/test_literals.py -v`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `literals.py`**

```python
"""Trino SQL literal builders and batched INSERT statements (Phase 5 spec §4).

Values travel as Python objects and are rendered as *typed* SQL literals so a
batched multi-row INSERT is a single deterministic statement per chunk — no
driver-side parameter protocol involved.
"""

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal

from omni_retail.lakehouse.bronze.specs import BronzeTableSpec

#: Rows per multi-row INSERT statement (Phase 5 design spec §4).
ROWS_PER_STATEMENT = 500

Row = Mapping[str, object]


def varchar_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def timestamp_literal(value: datetime) -> str:
    """``TIMESTAMP '... UTC'`` literal; naive values are assumed UTC."""
    moment = value if value.tzinfo else value.replace(tzinfo=UTC)
    return f"TIMESTAMP '{moment.astimezone(UTC):%Y-%m-%d %H:%M:%S.%f} UTC'"


def date_literal(value: date) -> str:
    return f"DATE '{value:%Y-%m-%d}'"


def sql_literal(value: object, trino_type: str) -> str:
    """Render one Python value as a Trino SQL literal for ``trino_type``."""
    if value is None:
        return "null"
    if trino_type.startswith("varchar"):
        if not isinstance(value, str):
            raise TypeError(f"varchar column expects str, got {type(value).__name__}")
        return varchar_literal(value)
    if trino_type.startswith("decimal"):
        if isinstance(value, bool) or not isinstance(value, Decimal):
            raise TypeError(f"decimal column expects Decimal, got {type(value).__name__}")
        return f"DECIMAL '{value}'"
    if trino_type.startswith("timestamp"):
        if not isinstance(value, datetime):
            raise TypeError(f"timestamp column expects datetime, got {type(value).__name__}")
        return timestamp_literal(value)
    if trino_type == "date":
        if not isinstance(value, date) or isinstance(value, datetime):
            raise TypeError(f"date column expects datetime.date, got {type(value).__name__}")
        return date_literal(value)
    if trino_type == "boolean":
        if not isinstance(value, bool):
            raise TypeError(f"boolean column expects bool, got {type(value).__name__}")
        return "true" if value else "false"
    if trino_type in ("bigint", "integer"):
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{trino_type} column expects int, got {type(value).__name__}")
        return str(value)
    if trino_type == "double":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"double column expects float, got {type(value).__name__}")
        return repr(float(value))
    raise TypeError(f"unsupported trino type for literals: {trino_type!r}")


def insert_statements(
    spec: BronzeTableSpec,
    rows: Sequence[Row],
    *,
    catalog: str = "iceberg",
    schema: str = "bronze",
    rows_per_statement: int = ROWS_PER_STATEMENT,
) -> list[str]:
    """Batched multi-row INSERTs covering every row in declaration order."""
    if rows_per_statement < 1:
        raise ValueError(f"rows_per_statement must be >= 1, got {rows_per_statement}")
    columns = spec.all_columns
    names = [column.name for column in columns]
    for row in rows:
        missing = [name for name in names if name not in row]
        if missing:
            raise ValueError(f"{spec.name}: row missing columns {missing}")

    column_list = ", ".join(f'"{name}"' for name in names)
    header = f"insert into {catalog}.{schema}.{spec.name} ({column_list}) values "

    statements: list[str] = []
    for start in range(0, len(rows), rows_per_statement):
        chunk = rows[start : start + rows_per_statement]
        tuples = ", ".join(
            "("
            + ", ".join(
                sql_literal(row[name], column.trino_type)
                for name, column in zip(names, columns, strict=True)
            )
            + ")"
            for row in chunk
        )
        statements.append(header + tuples)
    return statements
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/lakehouse/bronze/test_literals.py -v`
Expected: PASS (15 tests).

- [ ] **Step 5: Commit**

```bash
git add src/omni_retail/lakehouse/bronze/literals.py tests/unit/lakehouse/bronze/test_literals.py
git commit -m "feat(lakehouse): Trino SQL literals and batched inserts (Phase 5 slice 1)"
```

---

### Task 4: Raw-object readers (Parquet + API JSON)

**Files:**
- Create: `src/omni_retail/lakehouse/bronze/readers.py`
- Test: `tests/unit/lakehouse/bronze/test_readers.py`

**Interfaces:**
- Consumes: `BronzeTableSpec`/`BronzeColumnSpec` (Task 2); `ObjectStorage` protocol; `BUCKET_ARCHIVE`.
- Produces: `Row = dict[str, object]`; `BronzeReadError(Exception)`; `list_data_objects(storage, spec, logical_date) -> tuple[str, ...]`; `read_rows(spec, object_key, body) -> list[Row]` (dispatch by spec.kind); `read_parquet_rows(spec, object_key, body)`; `read_json_rows(spec, object_key, body)`.

- [ ] **Step 1: Write the failing tests** (`tests/unit/lakehouse/bronze/test_readers.py`)

```python
"""Unit tests for the raw-object readers (fixtures only, no network)."""

import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fakes.storage import FakeStorage
from omni_retail.ingestion.common.paths import BUCKET_ARCHIVE
from omni_retail.ingestion.postgres_snapshot.tables import table_by_name
from omni_retail.lakehouse.bronze.readers import (
    BronzeReadError,
    list_data_objects,
    read_json_rows,
    read_parquet_rows,
    read_rows,
)
from omni_retail.lakehouse.bronze.specs import TABLES

LOGICAL_DATE = date(2026, 9, 18)
SNAPSHOT_ORDERS = table_by_name("orders")


def orders_parquet_body() -> bytes:
    rows = [
        (
            1,
            9001,
            "paid",
            "USD",
            Decimal("1.50"),
            Decimal("10.00"),
            datetime(2026, 9, 17, 10, 0, tzinfo=UTC),
            datetime(2026, 9, 17, 10, 5, tzinfo=UTC),
        ),
        (
            2,
            9002,
            "shipped",
            "EUR",
            Decimal("2.00"),
            Decimal("20.00"),
            datetime(2026, 9, 17, 11, 0, tzinfo=UTC),
            datetime(2026, 9, 17, 11, 5, tzinfo=UTC),
        ),
    ]
    arrays = [
        pa.array([row[index] for row in rows], type=column.type)
        for index, column in enumerate(SNAPSHOT_ORDERS.columns)
    ]
    table = pa.Table.from_arrays(arrays, schema=SNAPSHOT_ORDERS.arrow_schema())
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()  # type: ignore[no-any-return]


ORDERS_KEY = "postgres/orders/2026/09/18/data.parquet"


def test_parquet_reader_returns_source_rows() -> None:
    rows = read_parquet_rows(TABLES["orders"], ORDERS_KEY, orders_parquet_body())
    assert len(rows) == 2
    assert rows[0]["order_id"] == 1
    assert rows[0]["order_total"] == Decimal("10.00")
    assert rows[1]["currency"] == "EUR"


def test_parquet_reader_rejects_schema_drift() -> None:
    schema = pa.schema([pa.field("order_id", pa.int64()), pa.field("surprise", pa.string())])
    table = pa.Table.from_arrays(
        [pa.array([1], pa.int64()), pa.array(["x"], pa.string())], schema=schema
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    body = sink.getvalue().to_pybytes()
    with pytest.raises(BronzeReadError, match="schema drift"):
        read_parquet_rows(TABLES["orders"], ORDERS_KEY, body)


def test_read_rows_dispatches_by_spec_kind() -> None:
    rows = read_rows(TABLES["orders"], ORDERS_KEY, orders_parquet_body())
    assert len(rows) == 2


def test_json_reader_flattens_fx_page() -> None:
    page = {
        "base": "EUR",
        "date": "2026-09-18",
        "page": 1,
        "page_size": 100,
        "total_count": 1,
        "rates": [{"currency": "USD", "rate": 1.091234}],
    }
    rows = read_json_rows(
        TABLES["fx_rates"], "api/fx-rates/20260918/page_0001.json", json.dumps(page).encode()
    )
    assert rows == [{"currency": "USD", "rate": Decimal("1.091234")}]


def test_json_reader_parses_campaign_dates_and_decimals() -> None:
    page = {
        "campaigns": [
            {
                "campaign_id": "CMP-0001",
                "name": "Summer Sale",
                "channel": "search",
                "status": "active",
                "start_date": "2026-01-15",
                "end_date": "2026-03-15",
                "budget_eur": 25000.5,
                "spend_eur": 12000.25,
                "impressions": 150000,
                "clicks": 3000,
            }
        ]
    }
    rows = read_json_rows(
        TABLES["campaigns"],
        "api/marketing-campaigns/20260918/page_0001.json",
        json.dumps(page).encode(),
    )
    assert rows[0]["start_date"] == date(2026, 1, 15)
    assert rows[0]["end_date"] == date(2026, 3, 15)
    assert rows[0]["budget_eur"] == Decimal("25000.5")
    assert rows[0]["impressions"] == 150000


def test_json_reader_parses_delivery_datetimes_and_nulls() -> None:
    page = {
        "deliveries": [
            {
                "delivery_id": "DLV-000001",
                "order_id": "ORD-123456",
                "carrier": "DHL",
                "status": "in_transit",
                "shipped_at": "2026-07-01T05:30:00+00:00",
                "delivered_at": None,
                "updated_at": "2026-07-02T05:30:00+00:00",
            }
        ]
    }
    rows = read_json_rows(
        TABLES["deliveries"],
        "api/deliveries/20260918/page_0001.json",
        json.dumps(page).encode(),
    )
    assert rows[0]["shipped_at"] == datetime(2026, 7, 1, 5, 30, tzinfo=UTC)
    assert rows[0]["delivered_at"] is None


def test_json_reader_rejects_bad_envelope() -> None:
    with pytest.raises(BronzeReadError, match="envelope field"):
        read_json_rows(TABLES["fx_rates"], "key", b'{"nope": []}')


def test_json_reader_rejects_missing_row_field() -> None:
    page = {"rates": [{"currency": "USD"}]}
    with pytest.raises(BronzeReadError, match="missing field 'rate'"):
        read_json_rows(TABLES["fx_rates"], "key", json.dumps(page).encode())


def test_json_reader_rejects_non_nullable_null() -> None:
    page = {"rates": [{"currency": None, "rate": 1.0}]}
    with pytest.raises(BronzeReadError, match="not nullable"):
        read_json_rows(TABLES["fx_rates"], "key", json.dumps(page).encode())


def test_json_reader_rejects_invalid_json() -> None:
    with pytest.raises(BronzeReadError, match="invalid JSON"):
        read_json_rows(TABLES["fx_rates"], "key", b"{not json")


def test_list_data_objects_filters_by_suffix() -> None:
    storage = FakeStorage()
    storage.put_object(BUCKET_ARCHIVE, ORDERS_KEY, orders_parquet_body())
    storage.put_object(
        BUCKET_ARCHIVE,
        "_manifests/postgres-orders/postgres-orders-20260918.json",
        b"{}",
    )
    storage.put_object(BUCKET_ARCHIVE, "postgres/orders/2026/09/17/data.parquet", b"other-day")

    assert list_data_objects(storage, TABLES["orders"], LOGICAL_DATE) == (ORDERS_KEY,)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/lakehouse/bronze/test_readers.py -v`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `readers.py`**

```python
"""Raw-object readers: PG snapshot Parquet and API JSON pages (Phase 5 spec §4).

Readers are strict on purpose: a raw object that does not match the Bronze
contract (column names for Parquet, envelope shape and field types for JSON)
raises :class:`BronzeReadError` before any partition is modified.
"""

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from omni_retail.ingestion.common.paths import BUCKET_ARCHIVE
from omni_retail.ingestion.common.storage import ObjectStorage
from omni_retail.lakehouse.bronze.specs import BronzeColumnSpec, BronzeTableSpec

Row = dict[str, object]


class BronzeReadError(Exception):
    """Raw object does not match the Bronze contract (schema drift / bad envelope)."""


def list_data_objects(
    storage: ObjectStorage, spec: BronzeTableSpec, logical_date: date
) -> tuple[str, ...]:
    """Sorted raw data-object keys of the logical date (manifests excluded by suffix)."""
    keys = storage.list_object_keys(BUCKET_ARCHIVE, spec.object_prefix(logical_date))
    suffix = "." + spec.data_object_suffix
    return tuple(key for key in keys if key.endswith(suffix))


def read_rows(spec: BronzeTableSpec, object_key: str, body: bytes) -> list[Row]:
    """Dispatch to the raw-format reader declared by the spec kind."""
    if spec.kind == "postgres":
        return read_parquet_rows(spec, object_key, body)
    return read_json_rows(spec, object_key, body)


def read_parquet_rows(spec: BronzeTableSpec, object_key: str, body: bytes) -> list[Row]:
    """Read a PG-snapshot Parquet object; column set must equal the spec exactly."""
    table = pq.read_table(pa.BufferReader(body))
    expected = {column.name for column in spec.columns}
    actual = set(table.column_names)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise BronzeReadError(f"{object_key}: schema drift (missing={missing}, extra={extra})")
    rows: list[Row] = []
    for record in table.to_pylist():
        rows.append(
            {
                column.name: _validate_native(object_key, column, record[column.name])
                for column in spec.columns
            }
        )
    return rows


def read_json_rows(spec: BronzeTableSpec, object_key: str, body: bytes) -> list[Row]:
    """Read one raw API page and flatten its envelope into typed rows."""
    try:
        payload: Any = json.loads(body)
    except json.JSONDecodeError as error:
        raise BronzeReadError(f"{object_key}: invalid JSON: {error}") from error
    envelope = payload.get(spec.envelope_field) if isinstance(payload, dict) else None
    if not isinstance(envelope, list):
        field = spec.envelope_field
        raise BronzeReadError(f"{object_key}: envelope field {field!r} must be a list")

    rows: list[Row] = []
    for position, record in enumerate(envelope):
        if not isinstance(record, dict):
            raise BronzeReadError(f"{object_key}: row {position} is not an object")
        row: Row = {}
        for column in spec.columns:
            if column.name not in record:
                raise BronzeReadError(f"{object_key}: row {position} missing field {column.name!r}")
            row[column.name] = _coerce_json(object_key, column, record[column.name])
        rows.append(row)
    return rows


def _type_family(trino_type: str) -> str:
    if trino_type.startswith("varchar"):
        return "varchar"
    if trino_type.startswith("decimal"):
        return "decimal"
    if trino_type.startswith("timestamp"):
        return "timestamp"
    known = {
        "bigint": "bigint",
        "integer": "bigint",
        "boolean": "boolean",
        "date": "date",
        "double": "double",
    }
    family = known.get(trino_type)
    if family is None:
        raise BronzeReadError(f"unsupported trino type in bronze spec: {trino_type!r}")
    return family


def _check_nullable(object_key: str, column: BronzeColumnSpec, value: object) -> None:
    if value is None and not column.nullable:
        raise BronzeReadError(f"{object_key}: {column.name} is null but not nullable")


def _validate_native(object_key: str, column: BronzeColumnSpec, value: object) -> object:
    """Parquet values already carry types; validate the Python-side family."""
    _check_nullable(object_key, column, value)
    if value is None:
        return None
    family = _type_family(column.trino_type)
    valid = (
        (family == "varchar" and isinstance(value, str))
        or (family == "bigint" and isinstance(value, int) and not isinstance(value, bool))
        or (family == "boolean" and isinstance(value, bool))
        or (family == "decimal" and isinstance(value, Decimal))
        or (family == "timestamp" and isinstance(value, datetime))
        or (family == "date" and isinstance(value, date) and not isinstance(value, datetime))
        or (family == "double" and isinstance(value, float))
    )
    if not valid:
        raise BronzeReadError(
            f"{object_key}: {column.name} expects {family}, got {type(value).__name__}"
        )
    return value


def _coerce_json(object_key: str, column: BronzeColumnSpec, value: object) -> object:
    """Coerce one JSON value into the Python type of the column's Trino type."""
    _check_nullable(object_key, column, value)
    if value is None:
        return None
    family = _type_family(column.trino_type)
    try:
        if family == "varchar":
            if not isinstance(value, str):
                raise BronzeReadError(f"expected str")
            return value
        if family == "bigint":
            if isinstance(value, bool) or not isinstance(value, int):
                raise BronzeReadError(f"expected int")
            return value
        if family == "boolean":
            if not isinstance(value, bool):
                raise BronzeReadError(f"expected bool")
            return value
        if family == "decimal":
            if isinstance(value, bool) or not isinstance(value, (int, float, str)):
                raise BronzeReadError(f"expected number")
            return Decimal(str(value))
        if family == "timestamp":
            if not isinstance(value, str):
                raise BronzeReadError(f"expected ISO-8601 string")
            parsed = datetime.fromisoformat(value)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        if family == "date":
            if not isinstance(value, str):
                raise BronzeReadError(f"expected ISO-8601 date string")
            return date.fromisoformat(value)
        if family == "double":
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise BronzeReadError(f"expected number")
            return float(value)
    except (ValueError, BronzeReadError) as error:
        raise BronzeReadError(
            f"{object_key}: {column.name} cannot be coerced to {family}: {error}"
        ) from error
    raise BronzeReadError(f"{object_key}: {column.name} unsupported family {family}")
```

Note: f-strings without placeholders (`f"expected str"`) will trip ruff F541 — write them as plain strings in the final code: `raise BronzeReadError("expected str")` etc.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/lakehouse/bronze/test_readers.py -v`
Expected: PASS (12 tests).

- [ ] **Step 5: Commit**

```bash
git add src/omni_retail/lakehouse/bronze/readers.py tests/unit/lakehouse/bronze/test_readers.py
git commit -m "feat(lakehouse): raw-object readers with strict schema checks (Phase 5 slice 1)"
```

---

### Task 5: Trino executor boundary, config, and Bronze DDL

**Files:**
- Create: `src/omni_retail/lakehouse/bronze/loader.py` (part 1: config, executor, DDL)
- Create: `tests/fakes/trino.py`
- Test: `tests/unit/lakehouse/bronze/test_loader.py` (part 1: DDL tests)

**Interfaces:**
- Consumes: `BronzeTableSpec`, `SERVICE_COLUMNS` (Task 2); `date_literal` (Task 3).
- Produces: `TrinoConfig(host="127.0.0.1", port=8080, catalog="iceberg", user="omni")` + `.from_env()`; `TrinoExecutor` protocol with `execute(sql) -> None`; `DbapiTrinoExecutor(config)` (context manager with `.close()`); `create_schema_sql(catalog) -> str`; `create_table_sql(spec, catalog) -> str`; `delete_partition_sql(spec, catalog, logical_date) -> str`; (Task 6 adds `load`, `LoadResult`, `LoadError`).
- Test fake: `tests/fakes/trino.py::FakeTrinoExecutor` with `.statements: list[str]`.

- [ ] **Step 1: Write the failing tests** (start of `tests/unit/lakehouse/bronze/test_loader.py`)

```python
"""Unit tests for the Bronze loader: DDL, ordering, idempotency (fakes only)."""

from datetime import UTC, date, datetime

import pytest

from omni_retail.lakehouse.bronze.loader import (
    DbapiTrinoExecutor,
    TrinoConfig,
    create_schema_sql,
    create_table_sql,
    delete_partition_sql,
)
from omni_retail.lakehouse.bronze.specs import TABLES

LOGICAL_DATE = date(2026, 9, 18)


def test_trino_config_defaults_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRINO_HOST", "trino")
    monkeypatch.setenv("TRINO_PORT", "8080")
    monkeypatch.setenv("TRINO_CATALOG", "iceberg")
    monkeypatch.setenv("TRINO_USER", "airflow")
    config = TrinoConfig.from_env()
    assert config == TrinoConfig(host="trino", port=8080, catalog="iceberg", user="airflow")


def test_trino_config_defaults_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("TRINO_HOST", "TRINO_PORT", "TRINO_CATALOG", "TRINO_USER"):
        monkeypatch.delenv(name, raising=False)
    assert TrinoConfig.from_env() == TrinoConfig()


def test_create_schema_sql_is_idempotent() -> None:
    assert create_schema_sql("iceberg") == "create schema if not exists iceberg.bronze"


def test_create_table_sql_declares_all_columns_and_partitioning() -> None:
    sql = create_table_sql(TABLES["orders"], "iceberg")
    assert sql.startswith("create table if not exists iceberg.bronze.orders (")
    assert '"order_id" bigint' in sql
    assert '"order_total" decimal(12,2)' in sql
    assert '"updated_at" timestamp(6) with time zone' in sql
    assert '"_batch_date" date' in sql
    assert sql.endswith("with (partitioning = ARRAY['_batch_date'])")


def test_delete_partition_sql_targets_logical_date() -> None:
    sql = delete_partition_sql(TABLES["orders"], "iceberg", LOGICAL_DATE)
    assert sql == ("delete from iceberg.bronze.orders where \"_batch_date\" = DATE '2026-09-18'")


def test_dbapi_executor_is_closable_context_manager() -> None:
    # Constructor must not connect eagerly (connection is lazy in trino client);
    # close() on a never-used executor must not raise.
    executor = DbapiTrinoExecutor(TrinoConfig())
    executor.close()
```

Note: check whether `trino.dbapi.connect()` connects eagerly. If it does, make `DbapiTrinoExecutor` lazy: store config, create the connection on first `execute()`/`close()` handles `None`. Prefer the lazy variant — implement it that way from the start (see Step 3).

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/lakehouse/bronze/test_loader.py -v`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `loader.py` (part 1)**

```python
"""Idempotent Bronze loader: raw archive objects -> Iceberg via Trino SQL.

Load semantics (Phase 5 design spec §4, §6):
- raw objects are read and verified (schema, manifest row count) BEFORE any
  DML, so a failed verification never touches the existing partition;
- ``load(source, date)`` replaces the day's partition: ``DELETE`` by
  ``_batch_date`` followed by batched ``INSERT`` statements — re-running the
  same logical date (retry, backfill) cannot create duplicate rows;
- an empty day (no raw objects) is a warning-level no-op.
"""

import logging
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Literal, Protocol

import trino

from omni_retail.ingestion.common.logging import context_logger
from omni_retail.ingestion.common.manifest import BatchManifest
from omni_retail.ingestion.common.paths import BUCKET_ARCHIVE, manifest_key
from omni_retail.ingestion.common.storage import ObjectNotFoundError, ObjectStorage
from omni_retail.lakehouse.bronze.literals import date_literal, insert_statements
from omni_retail.lakehouse.bronze.readers import BronzeReadError, list_data_objects, read_rows
from omni_retail.lakehouse.bronze.specs import (
    BATCH_DATE,
    BATCH_ID,
    INGESTED_AT,
    SCHEMA_BRONZE,
    SOURCE_OBJECT,
    BronzeTableSpec,
)

Clock = Callable[[], datetime]

logger = logging.getLogger(__name__)


class LoadError(Exception):
    """Explicit Bronze load failure (manifest missing, row-count mismatch)."""


@dataclass(frozen=True)
class TrinoConfig:
    """Connection settings for the local (unauthenticated) Trino coordinator."""

    host: str = "127.0.0.1"
    port: int = 8080
    catalog: str = "iceberg"
    user: str = "omni"

    @classmethod
    def from_env(cls) -> "TrinoConfig":
        return cls(
            host=os.environ.get("TRINO_HOST", cls.host),
            port=int(os.environ.get("TRINO_PORT", str(cls.port))),
            catalog=os.environ.get("TRINO_CATALOG", cls.catalog),
            user=os.environ.get("TRINO_USER", cls.user),
        )


class TrinoExecutor(Protocol):
    """SQL execution boundary (trino.dbapi connection or a test fake)."""

    def execute(self, sql: str) -> None: ...


class DbapiTrinoExecutor:
    """Lazily-connecting autocommit executor over one trino.dbapi connection."""

    def __init__(self, config: TrinoConfig) -> None:
        self._config = config
        self._connection: trino.dbapi.Connection | None = None

    def _connect(self) -> trino.dbapi.Connection:
        if self._connection is None:
            self._connection = trino.dbapi.connect(
                host=self._config.host,
                port=self._config.port,
                user=self._config.user,
                catalog=self._config.catalog,
            )
        return self._connection

    def execute(self, sql: str) -> None:
        cursor = self._connect().cursor()
        cursor.execute(sql)

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def __enter__(self) -> "DbapiTrinoExecutor":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


@dataclass(frozen=True)
class LoadResult:
    """Outcome of one (source, logical date) Bronze load."""

    source: str
    batch_id: str
    logical_date: date
    row_count: int
    status: Literal["loaded", "empty"]


def create_schema_sql(catalog: str) -> str:
    return f"create schema if not exists {catalog}.{SCHEMA_BRONZE}"


def create_table_sql(spec: BronzeTableSpec, catalog: str) -> str:
    columns = ", ".join(f'"{column.name}" {column.trino_type}' for column in spec.all_columns)
    return (
        f"create table if not exists {catalog}.{SCHEMA_BRONZE}.{spec.name} "
        f"({columns}) with (partitioning = ARRAY['_batch_date'])"
    )


def delete_partition_sql(spec: BronzeTableSpec, catalog: str, logical_date: date) -> str:
    return (
        f"delete from {catalog}.{SCHEMA_BRONZE}.{spec.name} "
        f'where "{BATCH_DATE}" = {date_literal(logical_date)}'
    )
```

Also create `tests/fakes/trino.py`:

```python
"""In-memory fake implementing the TrinoExecutor protocol for unit tests."""


class FakeTrinoExecutor:
    """Records every executed SQL statement in order (no Trino involved)."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql: str) -> None:
        self.statements.append(sql)

    def statements_matching(self, prefix: str) -> list[str]:
        return [sql for sql in self.statements if sql.startswith(prefix)]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/lakehouse/bronze/test_loader.py -v`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add src/omni_retail/lakehouse/bronze/loader.py tests/fakes/trino.py tests/unit/lakehouse/bronze/test_loader.py
git commit -m "feat(lakehouse): Trino executor boundary and Bronze DDL (Phase 5 slice 1)"
```

---

### Task 6: Idempotent load orchestration

**Files:**
- Modify: `src/omni_retail/lakehouse/bronze/loader.py` (add `load` + `_read_manifest`)
- Test: extend `tests/unit/lakehouse/bronze/test_loader.py`

**Interfaces:**
- Consumes: everything from Tasks 2–5.
- Produces: `load(storage, executor, spec, *, logical_date, clock=None, catalog="iceberg") -> LoadResult`; `_read_manifest` (private).

- [ ] **Step 1: Write the failing tests** (append to `test_loader.py`)

```python
import hashlib
import json
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

from fakes.storage import FakeStorage
from fakes.trino import FakeTrinoExecutor
from omni_retail.ingestion.common.manifest import BatchManifest
from omni_retail.ingestion.common.paths import (
    api_page_key,
    manifest_key,
    postgres_snapshot_key,
)
from omni_retail.ingestion.postgres_snapshot.tables import table_by_name
from omni_retail.lakehouse.bronze.loader import LoadError, load

ORDERS_SNAPSHOT = table_by_name("orders")


def fixed_clock() -> datetime:
    return datetime(2026, 9, 18, 9, 0, tzinfo=UTC)


def orders_rows(count: int) -> list[tuple[object, ...]]:
    return [
        (
            order_id,
            9000 + order_id,
            "paid",
            "USD",
            Decimal("1.50"),
            Decimal(f"{10 * order_id}.00"),
            datetime(2026, 9, 17, 10, 0, tzinfo=UTC),
            datetime(2026, 9, 17, 10, 5, tzinfo=UTC),
        )
        for order_id in range(1, count + 1)
    ]


def orders_parquet(rows: list[tuple[object, ...]]) -> bytes:
    arrays = [
        pa.array([row[index] for row in rows], type=column.type)
        for index, column in enumerate(ORDERS_SNAPSHOT.columns)
    ]
    table = pa.Table.from_arrays(arrays, schema=ORDERS_SNAPSHOT.arrow_schema())
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()  # type: ignore[no-any-return]


def write_manifest(storage: FakeStorage, *, source: str, batch_id: str, row_count: int) -> None:
    manifest = BatchManifest(
        batch_id=batch_id,
        source=source,
        source_kind="postgres" if source.startswith("postgres-") else "api",
        status="completed",
        object_key="unused",
        checksum=hashlib.sha256(b"unused").hexdigest(),
        size_bytes=1,
        ingested_at=fixed_clock(),
        logical_date=LOGICAL_DATE,
        row_count=row_count,
        rejected_row_count=0,
        schema_version="1.0",
    )
    storage.put_object("archive", manifest_key(source, batch_id), manifest.to_json().encode())


def seed_orders_batch(storage: FakeStorage, *, rows: int, manifest_rows: int | None = None) -> None:
    body = orders_parquet(orders_rows(rows))
    storage.put_object("archive", postgres_snapshot_key("orders", LOGICAL_DATE), body)
    write_manifest(
        storage,
        source="postgres-orders",
        batch_id=f"postgres-orders-{LOGICAL_DATE:%Y%m%d}",
        row_count=rows if manifest_rows is None else manifest_rows,
    )


def seed_fx_batch(storage: FakeStorage) -> None:
    pages = [
        {"rates": [{"currency": "USD", "rate": 1.09}, {"currency": "GBP", "rate": 0.85}]},
        {"rates": [{"currency": "JPY", "rate": 163.2}]},
    ]
    for number, page in enumerate(pages, start=1):
        storage.put_object(
            "archive", api_page_key("fx-rates", LOGICAL_DATE, number), json.dumps(page).encode()
        )
    write_manifest(
        storage,
        source="fx-rates",
        batch_id=f"fx-rates-{LOGICAL_DATE:%Y%m%d}",
        row_count=3,
    )


def test_load_postgres_batch_deletes_then_inserts() -> None:
    storage = FakeStorage()
    executor = FakeTrinoExecutor()
    seed_orders_batch(storage, rows=3)

    result = load(storage, executor, TABLES["orders"], logical_date=LOGICAL_DATE, clock=fixed_clock)

    assert result.status == "loaded"
    assert result.row_count == 3
    assert result.batch_id == "postgres-orders-20260918"
    assert executor.statements[0].startswith("create schema")
    assert executor.statements[1].startswith("create table")
    deletes = executor.statements_matching("delete from")
    inserts = executor.statements_matching("insert into")
    assert len(deletes) == 1
    assert len(inserts) == 1
    assert executor.statements.index(deletes[0]) < executor.statements.index(inserts[0])
    assert "'postgres-orders-20260918'" in inserts[0]
    assert "'postgres/orders/2026/09/18/data.parquet'" in inserts[0]


def test_load_rerun_replaces_partition_without_duplicates() -> None:
    storage = FakeStorage()
    executor = FakeTrinoExecutor()
    seed_orders_batch(storage, rows=3)

    load(storage, executor, TABLES["orders"], logical_date=LOGICAL_DATE, clock=fixed_clock)
    load(storage, executor, TABLES["orders"], logical_date=LOGICAL_DATE, clock=fixed_clock)

    assert len(executor.statements_matching("delete from")) == 2
    assert len(executor.statements_matching("insert into")) == 2
    assert executor.statements_matching("insert into")[0].count("), (") == 2


def test_load_api_batch_flattens_pages_and_tags_source_objects() -> None:
    storage = FakeStorage()
    executor = FakeTrinoExecutor()
    seed_fx_batch(storage)

    result = load(
        storage, executor, TABLES["fx_rates"], logical_date=LOGICAL_DATE, clock=fixed_clock
    )

    assert result.row_count == 3
    insert = executor.statements_matching("insert into")[0]
    assert "'api/fx-rates/20260918/page_0001.json'" in insert
    assert "'api/fx-rates/20260918/page_0002.json'" in insert


def test_load_row_count_mismatch_keeps_partition_untouched() -> None:
    storage = FakeStorage()
    executor = FakeTrinoExecutor()
    seed_orders_batch(storage, rows=3, manifest_rows=5)

    with pytest.raises(LoadError, match="row count mismatch"):
        load(storage, executor, TABLES["orders"], logical_date=LOGICAL_DATE, clock=fixed_clock)
    assert executor.statements == []


def test_load_missing_manifest_fails_before_dml() -> None:
    storage = FakeStorage()
    executor = FakeTrinoExecutor()
    storage.put_object(
        "archive", postgres_snapshot_key("orders", LOGICAL_DATE), orders_parquet(orders_rows(2))
    )

    with pytest.raises(LoadError, match="manifest not found"):
        load(storage, executor, TABLES["orders"], logical_date=LOGICAL_DATE, clock=fixed_clock)
    assert executor.statements == []


def test_load_empty_day_is_noop() -> None:
    storage = FakeStorage()
    executor = FakeTrinoExecutor()

    result = load(storage, executor, TABLES["orders"], logical_date=LOGICAL_DATE, clock=fixed_clock)

    assert result.status == "empty"
    assert result.row_count == 0
    assert executor.statements == []


def test_load_all_rows_share_one_ingested_at() -> None:
    storage = FakeStorage()
    executor = FakeTrinoExecutor()
    seed_orders_batch(storage, rows=3)

    load(storage, executor, TABLES["orders"], logical_date=LOGICAL_DATE, clock=fixed_clock)

    insert = executor.statements_matching("insert into")[0]
    assert insert.count("TIMESTAMP '2026-09-18 09:00:00.000000 UTC'") == 3
```

Note the bucket literal: `FakeStorage` and `list_data_objects` both use the constant `"archive"`/`BUCKET_ARCHIVE` — import `BUCKET_ARCHIVE` and use it instead of the string literal in the helpers to stay consistent.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/lakehouse/bronze/test_loader.py -v`
Expected: new tests FAIL with `ImportError` (LoadError/load not defined).

- [ ] **Step 3: Implement `load` (append to `loader.py`)**

```python
def load(
    storage: ObjectStorage,
    executor: TrinoExecutor,
    spec: BronzeTableSpec,
    *,
    logical_date: date,
    clock: Clock | None = None,
    catalog: str = "iceberg",
) -> LoadResult:
    """Load one (source, logical date) batch into Bronze; idempotent per day."""
    effective_clock: Clock = clock or (lambda: datetime.now(UTC))
    log = context_logger(__name__, source=spec.source_name, logical_date=logical_date.isoformat())
    batch_id = spec.batch_id(logical_date)

    objects = list_data_objects(storage, spec, logical_date)
    if not objects:
        log.warning("bronze batch empty: no raw objects under %s", spec.object_prefix(logical_date))
        return LoadResult(spec.source_key, batch_id, logical_date, 0, "empty")

    manifest = _read_manifest(storage, spec, logical_date)
    ingested_at = effective_clock()
    rows: list[dict[str, object]] = []
    for object_key in objects:
        body = storage.get_object(BUCKET_ARCHIVE, object_key)
        for row in read_rows(spec, object_key, body):
            rows.append(
                {
                    **row,
                    BATCH_ID: batch_id,
                    BATCH_DATE: logical_date,
                    SOURCE_OBJECT: object_key,
                    INGESTED_AT: ingested_at,
                }
            )

    if len(rows) != manifest.row_count:
        raise LoadError(
            f"{spec.source_key}: row count mismatch for {logical_date}: "
            f"raw rows={len(rows)} manifest rows={manifest.row_count} "
            "(partition not modified)"
        )

    executor.execute(create_schema_sql(catalog))
    executor.execute(create_table_sql(spec, catalog))
    executor.execute(delete_partition_sql(spec, catalog, logical_date))
    statements = insert_statements(spec, rows, catalog=catalog)
    for statement in statements:
        executor.execute(statement)
    log.info(
        "bronze load completed: batch_id=%s objects=%d row_count=%d statements=%d",
        batch_id,
        len(objects),
        len(rows),
        len(statements),
    )
    return LoadResult(spec.source_key, batch_id, logical_date, len(rows), "loaded")


def _read_manifest(
    storage: ObjectStorage, spec: BronzeTableSpec, logical_date: date
) -> BatchManifest:
    key = manifest_key(spec.source_name, spec.batch_id(logical_date))
    try:
        body = storage.get_object(BUCKET_ARCHIVE, key)
    except ObjectNotFoundError as error:
        raise LoadError(f"manifest not found: s3://{BUCKET_ARCHIVE}/{key}") from error
    return BatchManifest.from_json(body.decode())
```

- [ ] **Step 4: Run the full loader test module**

Run: `uv run pytest tests/unit/lakehouse/bronze/test_loader.py -v`
Expected: PASS (13 tests).

- [ ] **Step 5: Commit**

```bash
git add src/omni_retail/lakehouse/bronze/loader.py tests/unit/lakehouse/bronze/test_loader.py
git commit -m "feat(lakehouse): idempotent Bronze load with manifest verification (Phase 5 slice 1)"
```

---

### Task 7: Bronze loader CLI and Makefile target

**Files:**
- Create: `src/omni_retail/lakehouse/bronze/cli.py`
- Create: `src/omni_retail/lakehouse/bronze/__main__.py`
- Modify: `Makefile`
- Test: `tests/unit/lakehouse/bronze/test_cli.py`

**Interfaces:**
- Consumes: `load`, `DbapiTrinoExecutor`, `TrinoConfig`, `LoadError` (Tasks 5–6); `BronzeReadError`; `spec_by_source`, `TABLES`; `BotoObjectStorage`, `StorageConfig`, `StorageError`; `configure_logging`, `context_logger`.
- Produces: `build_parser()`; `run_one(spec, storage, executor, logical_date, catalog="iceberg") -> int`; `run_all(storage, executor, logical_date, catalog="iceberg") -> int`; `main(argv=None) -> int`; CLI `run --source <key> --date <d>` and `run-all --date <d>`; Makefile target `bronze-load`.

- [ ] **Step 1: Write the failing tests** (`tests/unit/lakehouse/bronze/test_cli.py`)

```python
"""Unit tests for the Bronze loader CLI (fakes; no network)."""

from datetime import date

import pytest

from fakes.storage import FakeStorage
from fakes.trino import FakeTrinoExecutor
from omni_retail.lakehouse.bronze import cli
from omni_retail.lakehouse.bronze.cli import build_parser, main, run_all, run_one
from omni_retail.lakehouse.bronze.loader import LoadError
from omni_retail.lakehouse.bronze.specs import TABLES

LOGICAL_DATE = date(2026, 9, 18)


def test_parser_run_parses_source_and_date() -> None:
    args = build_parser().parse_args(["run", "--source", "orders", "--date", "2026-09-18"])
    assert args.command == "run"
    assert args.source == "orders"
    assert args.date == LOGICAL_DATE


def test_parser_run_all_parses_date() -> None:
    args = build_parser().parse_args(["run-all", "--date", "2026-09-18"])
    assert args.command == "run-all"
    assert args.date == LOGICAL_DATE


def test_run_one_returns_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[object] = []
    monkeypatch.setattr(cli, "load", lambda *a, **k: calls.append(a) or _result("loaded"))
    assert run_one(TABLES["orders"], FakeStorage(), FakeTrinoExecutor(), LOGICAL_DATE) == 0
    assert len(calls) == 1


def test_run_all_loads_every_source_and_tolerates_empty_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded: list[str] = []

    def fake_load(storage, executor, spec, *, logical_date, clock=None, catalog="iceberg"):
        loaded.append(spec.source_key)
        return _result("empty")

    monkeypatch.setattr(cli, "load", fake_load)
    assert run_all(FakeStorage(), FakeTrinoExecutor(), LOGICAL_DATE) == 0
    assert len(loaded) == 10


def test_run_all_fails_fast_on_load_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_load(storage, executor, spec, *, logical_date, clock=None, catalog="iceberg"):
        raise LoadError("boom")

    monkeypatch.setattr(cli, "load", fake_load)
    with pytest.raises(LoadError):
        run_all(FakeStorage(), FakeTrinoExecutor(), LOGICAL_DATE)


def test_main_unknown_source_returns_one() -> None:
    assert main(["run", "--source", "nope", "--date", "2026-09-18"]) == 1


def _result(status: str):  # minimal LoadResult stand-in factory
    from omni_retail.lakehouse.bronze.loader import LoadResult

    return LoadResult("orders", "postgres-orders-20260918", LOGICAL_DATE, 0, status)
```

`test_main_unknown_source_returns_one` relies on `main` resolving the spec BEFORE constructing storage/executor clients (see implementation).

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/lakehouse/bronze/test_cli.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement `cli.py` and `__main__.py`**

`src/omni_retail/lakehouse/bronze/cli.py`:

```python
"""Command-line interface for the Bronze loader (Phase 5 design spec §4)."""

import argparse
import contextlib
import logging
import sys
from datetime import date

from omni_retail.ingestion.common.logging import configure_logging, context_logger
from omni_retail.ingestion.common.storage import (
    BotoObjectStorage,
    ObjectStorage,
    StorageConfig,
    StorageError,
)
from omni_retail.lakehouse.bronze.loader import (
    DbapiTrinoExecutor,
    LoadError,
    TrinoConfig,
    TrinoExecutor,
    load,
)
from omni_retail.lakehouse.bronze.readers import BronzeReadError
from omni_retail.lakehouse.bronze.specs import BronzeTableSpec, TABLES, spec_by_source

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m omni_retail.lakehouse.bronze",
        description="Load raw archive objects into Iceberg Bronze tables via Trino.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="load one logical date of one source")
    run_parser.add_argument(
        "--source",
        required=True,
        help="OLTP table name (orders, ...) or API source name (fx-rates, ...)",
    )
    run_parser.add_argument(
        "--date",
        type=date.fromisoformat,
        required=True,
        help="logical date (YYYY-MM-DD) addressed by batch_id and the partition",
    )

    run_all_parser = subparsers.add_parser(
        "run-all", help="load every source for one logical date; fails fast, restartable"
    )
    run_all_parser.add_argument("--date", type=date.fromisoformat, required=True)
    return parser


def run_one(
    spec: BronzeTableSpec,
    storage: ObjectStorage,
    executor: TrinoExecutor,
    logical_date: date,
    catalog: str = "iceberg",
) -> int:
    """Load one source for the logical date; returns a process exit code."""
    result = load(storage, executor, spec, logical_date=logical_date, catalog=catalog)
    return 0


def run_all(
    storage: ObjectStorage, executor: TrinoExecutor, logical_date: date, catalog: str = "iceberg"
) -> int:
    """Load every registered source; empty days are warnings, errors fail fast."""
    log = context_logger(__name__, logical_date=logical_date.isoformat())
    for spec in TABLES.values():
        result = load(storage, executor, spec, logical_date=logical_date, catalog=catalog)
        if result.status == "loaded":
            log.info("run-all progress: source=%s row_count=%d", result.source, result.row_count)
    return 0


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = build_parser().parse_args(argv)
    try:
        # Resolve the spec first so unknown sources fail before any client is built.
        spec = spec_by_source(args.source) if args.command == "run" else None
        storage = BotoObjectStorage(StorageConfig.from_env())
        with contextlib.closing(DbapiTrinoExecutor(TrinoConfig.from_env())) as executor:
            if args.command == "run":
                assert spec is not None
                return run_one(spec, storage, executor, args.date)
            return run_all(storage, executor, args.date)
    except (LoadError, BronzeReadError, StorageError, ValueError) as error:
        logger.error("%s failed: %s", args.command, error)
        return 1


if __name__ == "__main__":
    sys.exit(main())
```

`src/omni_retail/lakehouse/bronze/__main__.py`:

```python
"""``python -m omni_retail.lakehouse.bronze`` entry point."""

import sys

from omni_retail.lakehouse.bronze.cli import main

if __name__ == "__main__":
    sys.exit(main())
```

Makefile (add after `ingest-api`, same env-sourcing pattern; and extend `.PHONY`):

```make
bronze-load: ## Load raw archive data into Iceberg Bronze. ARGS="run --source orders --date 2026-09-18" or "run-all --date 2026-09-18"
	@bash -c 'set -a; source .env; set +a; $(UV) run python -m omni_retail.lakehouse.bronze $(ARGS)'
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/lakehouse/bronze/test_cli.py -v`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add src/omni_retail/lakehouse/bronze/cli.py src/omni_retail/lakehouse/bronze/__main__.py Makefile tests/unit/lakehouse/bronze/test_cli.py
git commit -m "feat(lakehouse): bronze loader CLI run/run-all and make target (Phase 5 slice 1)"
```

---

### Task 8: dbt project skeleton, staging models, guards, CI

**Files:**
- Create: `dbt/dbt_project.yml`
- Create: `dbt/profiles.yml`
- Create: `dbt/macros/generate_schema_name.sql`
- Create: `dbt/models/staging/sources.yml`
- Create: `dbt/models/staging/stg_categories.sql`, `stg_products.sql`, `stg_customers.sql`, `stg_orders.sql`, `stg_order_items.sql`, `stg_payments.sql`, `stg_shipments.sql`
- Create: `dbt/.gitignore`
- Modify: `Makefile` (dbt-parse target)
- Modify: `.github/workflows/ci.yml` (dbt parse step)
- Modify: `tests/test_compose_guards.py` (Trino env keys + dbt guards)

**Interfaces:**
- Consumes: Bronze tables from Tasks 2–6 (dbt reads them as sources).
- Produces: dbt profile `omni_retail` (target `dev`, type `trino`, env-driven); views `iceberg.silver.stg_<table>` over `iceberg.bronze.<table>`; `make dbt-parse`; CI step "dbt parse"; guard tests `EXPECTED_TRINO_ENV_KEYS`.

- [ ] **Step 1: Write the failing guard tests** (extend `tests/test_compose_guards.py`)

Add near the other EXPECTED_* sets:

```python
# Trino/dbt configuration documented for Phase 5 (design spec §8).
EXPECTED_TRINO_ENV_KEYS = {"TRINO_HOST", "TRINO_PORT", "TRINO_CATALOG", "TRINO_USER"}

DBT_DIR = REPO_ROOT / "dbt"
DBT_STAGING_MODELS = {
    "stg_categories.sql",
    "stg_customers.sql",
    "stg_order_items.sql",
    "stg_orders.sql",
    "stg_payments.sql",
    "stg_products.sql",
    "stg_shipments.sql",
}
```

Add tests at the end of the file:

```python
def test_env_example_documents_trino_variables() -> None:
    missing = EXPECTED_TRINO_ENV_KEYS - _env_example_keys()
    assert not missing, f"trino env vars missing from .env.example: {sorted(missing)}"


def test_dbt_profiles_are_env_driven_and_secret_free() -> None:
    profiles = yaml.safe_load((DBT_DIR / "profiles.yml").read_text(encoding="utf-8"))
    output = profiles["omni_retail"]["outputs"]["dev"]
    assert output["type"] == "trino"
    assert output["http_scheme"] == "http"
    for forbidden in ("password", "key_file", "access_token", "credentials"):
        assert forbidden not in output, f"profiles.yml must not contain {forbidden!r}"
    for env_driven in ("host", "user", "catalog", "schema"):
        assert "env_var" in str(output[env_driven]), f"{env_driven} must come from env_var"


def test_dbt_project_and_staging_skeleton_exist() -> None:
    assert (DBT_DIR / "dbt_project.yml").is_file()
    assert (DBT_DIR / "profiles.yml").is_file()
    assert (DBT_DIR / "models" / "staging" / "sources.yml").is_file()
    models = {path.name for path in (DBT_DIR / "models" / "staging").glob("stg_*.sql")}
    assert models == DBT_STAGING_MODELS
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_compose_guards.py -k "trino or dbt" -v`
Expected: FAIL (files/keys missing).

- [ ] **Step 3: Create the dbt skeleton**

`dbt/dbt_project.yml`:

```yaml
name: omni_retail
version: "1.0.0"
config-version: 2

profile: omni_retail

model-paths: ["models"]
macro-paths: ["macros"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  omni_retail:
    staging:
      +materialized: view
      +schema: silver
```

`dbt/profiles.yml` (committed; no secrets — all values env-driven with host-side defaults):

```yaml
omni_retail:
  target: dev
  outputs:
    dev:
      type: trino
      host: "{{ env_var('TRINO_HOST', '127.0.0.1') }}"
      port: "{{ env_var('TRINO_PORT', '8080') | int }}"
      user: "{{ env_var('TRINO_USER', 'omni') }}"
      catalog: "{{ env_var('TRINO_CATALOG', 'iceberg') }}"
      schema: "{{ env_var('DBT_SCHEMA', 'silver') }}"
      threads: 4
      http_scheme: http
```

`dbt/macros/generate_schema_name.sql` (layers map 1:1 to schemas — spec §2):

```sql
{# Layers are explicit schemas (silver/gold/analytics): use the model's
   custom schema verbatim instead of the default `<target>_<schema>` prefix. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

`dbt/models/staging/sources.yml`:

```yaml
version: 2

sources:
  - name: bronze
    catalog: iceberg
    schema: bronze
    freshness:
      warn_after: {count: 48, period: hour}
      error_after: {count: 7, period: day}
    loaded_at_field: _ingested_at
    tables:
      - name: categories
      - name: products
      - name: customers
      - name: orders
      - name: order_items
      - name: payments
      - name: shipments
```

If `dbt parse` rejects the `catalog:` key for sources, remove the line (the source then resolves against the target catalog `iceberg`, which is identical here) — record which variant was kept in the commit message.

The 7 staging models — pass-through views, explicit columns, service columns preserved (grain documented per model). `dbt/models/staging/stg_orders.sql`:

```sql
{{ config(materialized='view') }}

-- Grain: one row per OLTP snapshot record (order_id, _batch_id).
select
    order_id,
    customer_id,
    status,
    currency,
    shipping_cost,
    order_total,
    created_at,
    updated_at,
    _batch_id,
    _batch_date,
    _source_object,
    _ingested_at
from {{ source('bronze', 'orders') }}
```

`stg_categories.sql` (business columns: category_id, name, parent_category_id, created_at, updated_at), `stg_products.sql` (product_id, sku, name, category_id, brand, unit_price, unit_cost, is_active, created_at, updated_at), `stg_customers.sql` (customer_id, email, first_name, last_name, region, city, status, segment, registered_at, created_at, updated_at), `stg_order_items.sql` (order_item_id, order_id, product_id, quantity, unit_price, line_total, created_at, updated_at), `stg_payments.sql` (payment_id, order_id, method, status, amount, transaction_id, created_at, updated_at), `stg_shipments.sql` (shipment_id, order_id, carrier, tracking_number, status, shipped_at, delivered_at, created_at, updated_at) — same shape: grain comment, explicit business columns, then the four service columns, `from {{ source('bronze', '<table>') }}`.

`dbt/.gitignore`:

```
target/
logs/
dbt_packages/
```

- [ ] **Step 4: Add the Makefile target and CI step**

Makefile (with `.PHONY` updated):

```make
dbt-parse: ## Parse the dbt project offline (no live stack needed)
	$(UV) run dbt parse --project-dir dbt --profiles-dir dbt
```

`.github/workflows/ci.yml` — add after the Pytest step in the `ci` job:

```yaml
      - name: dbt parse (offline manifest check)
        run: uv run dbt parse --project-dir dbt --profiles-dir dbt
```

- [ ] **Step 5: Validate**

```bash
uv run pytest tests/test_compose_guards.py -v
make dbt-parse
uv run ruff check . && uv run ruff format --check . && uv run mypy src tests
```

Expected: guards PASS; `dbt parse` succeeds offline (manifest written under `dbt/target/`, git-ignored).

- [ ] **Step 6: Commit**

```bash
git add dbt Makefile .github/workflows/ci.yml tests/test_compose_guards.py
git commit -m "feat(dbt): project skeleton with OLTP staging views and offline parse in CI (Phase 5 slice 1)"
```

---

### Task 9: Live integration test and dbt build targets

**Files:**
- Create: `tests/integration/test_bronze_load.py`
- Modify: `Makefile` (dbt-build, dbt-test)

**Interfaces:**
- Consumes: `load`, `DbapiTrinoExecutor`, `TrinoConfig`, `TABLES`/`spec_by_source`; `live_storage` fixture from `tests/integration/conftest.py`; dbt project from Task 8.
- Produces: integration coverage for spec §9 slice 1: bronze load + idempotent re-run + `dbt run` staging against the live stack; Makefile targets `dbt-build`, `dbt-test`.

- [ ] **Step 1: Write the test** (`tests/integration/test_bronze_load.py`)

```python
"""Live integration: archive raw -> Bronze load -> idempotent re-run -> dbt staging.

Requires the core profile (MinIO, Polaris, Trino) and OMNI_INTEGRATION=1
(``make up`` then ``make integration``). Seeded raw objects are deterministic;
bronze/silver objects are dropped before and after each test.
"""

import hashlib
import json
import os
import subprocess
from collections.abc import Generator
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import trino

from omni_retail.ingestion.common.manifest import BatchManifest
from omni_retail.ingestion.common.paths import (
    BUCKET_ARCHIVE,
    api_page_key,
    manifest_key,
    postgres_snapshot_key,
)
from omni_retail.ingestion.common.storage import BotoObjectStorage
from omni_retail.ingestion.postgres_snapshot.tables import table_by_name
from omni_retail.lakehouse.bronze.loader import DbapiTrinoExecutor, TrinoConfig, load
from omni_retail.lakehouse.bronze.specs import spec_by_source

REPO_ROOT = Path(__file__).resolve().parents[2]
LOGICAL_DATE = date(2026, 9, 10)
ORDERS_SNAPSHOT = table_by_name("orders")


def trino_scalar(sql: str) -> object:
    config = TrinoConfig.from_env()
    connection = trino.dbapi.connect(
        host=config.host, port=config.port, user=config.user, catalog=config.catalog
    )
    cursor = connection.cursor()
    cursor.execute(sql)
    row = cursor.fetchone()
    connection.close()
    assert row is not None
    return row[0]


def purge_raw(storage: BotoObjectStorage) -> None:
    for prefix in (
        f"postgres/orders/{LOGICAL_DATE:%Y/%m/%d}/",
        f"_manifests/postgres-orders/",
        f"api/fx-rates/{LOGICAL_DATE:%Y%m%d}/",
        f"_manifests/fx-rates/",
    ):
        for key in storage.list_object_keys(BUCKET_ARCHIVE, prefix):
            storage.delete_object(BUCKET_ARCHIVE, key)


@pytest.fixture()
def clean_bronze(live_storage: BotoObjectStorage) -> Generator[None, None, None]:
    purge_raw(live_storage)
    yield
    for statement in (
        "drop table if exists iceberg.bronze.orders",
        "drop table if exists iceberg.bronze.fx_rates",
        "drop view if exists iceberg.silver.stg_orders",
    ):
        trino_scalar(statement)
    purge_raw(live_storage)


def orders_rows(count: int) -> list[tuple[object, ...]]:
    return [
        (
            order_id,
            9000 + order_id,
            "paid",
            "USD",
            Decimal("1.50"),
            Decimal(f"{10 * order_id}.00"),
            datetime(2026, 9, 9, 10, 0, tzinfo=UTC),
            datetime(2026, 9, 9, 10, 5, tzinfo=UTC),
        )
        for order_id in range(1, count + 1)
    ]


def seed_orders(storage: BotoObjectStorage, rows: int) -> None:
    data = orders_rows(rows)
    arrays = [
        pa.array([row[index] for row in data], type=column.type)
        for index, column in enumerate(ORDERS_SNAPSHOT.columns)
    ]
    table = pa.Table.from_arrays(arrays, schema=ORDERS_SNAPSHOT.arrow_schema())
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    body = sink.getvalue().to_pybytes()
    storage.put_object(BUCKET_ARCHIVE, postgres_snapshot_key("orders", LOGICAL_DATE), body)
    manifest = BatchManifest(
        batch_id=f"postgres-orders-{LOGICAL_DATE:%Y%m%d}",
        source="postgres-orders",
        source_kind="postgres",
        status="completed",
        object_key=postgres_snapshot_key("orders", LOGICAL_DATE),
        checksum=hashlib.sha256(body).hexdigest(),
        size_bytes=len(body),
        ingested_at=datetime(2026, 9, 10, 8, 0, tzinfo=UTC),
        logical_date=LOGICAL_DATE,
        row_count=rows,
        rejected_row_count=0,
        schema_version="1.0",
    )
    storage.put_object(
        BUCKET_ARCHIVE,
        manifest_key("postgres-orders", manifest.batch_id),
        manifest.to_json().encode(),
    )


def test_postgres_bronze_load_is_idempotent(
    live_storage: BotoObjectStorage, clean_bronze: None
) -> None:
    seed_orders(live_storage, rows=3)

    with DbapiTrinoExecutor(TrinoConfig.from_env()) as executor:
        first = load(live_storage, executor, spec_by_source("orders"), logical_date=LOGICAL_DATE)
        second = load(live_storage, executor, spec_by_source("orders"), logical_date=LOGICAL_DATE)

    assert first.row_count == 3
    assert second.row_count == 3
    where = f"where \"_batch_date\" = DATE '{LOGICAL_DATE:%Y-%m-%d}'"
    assert trino_scalar(f"select count(*) from iceberg.bronze.orders {where}") == 3
    assert (
        trino_scalar(f'select distinct "_batch_id" from iceberg.bronze.orders {where}')
        == f"postgres-orders-{LOGICAL_DATE:%Y%m%d}"
    )


def test_api_bronze_load_flattens_pages(
    live_storage: BotoObjectStorage, clean_bronze: None
) -> None:
    pages = [
        {"rates": [{"currency": "USD", "rate": 1.091234}, {"currency": "GBP", "rate": 0.85}]},
        {"rates": [{"currency": "JPY", "rate": 163.2}]},
    ]
    for number, page in enumerate(pages, start=1):
        live_storage.put_object(
            BUCKET_ARCHIVE,
            api_page_key("fx-rates", LOGICAL_DATE, number),
            json.dumps(page).encode(),
        )
    manifest = BatchManifest(
        batch_id=f"fx-rates-{LOGICAL_DATE:%Y%m%d}",
        source="fx-rates",
        source_kind="api",
        status="completed",
        object_key=f"api/fx-rates/{LOGICAL_DATE:%Y%m%d}/",
        checksum=hashlib.sha256(b"pages").hexdigest(),
        size_bytes=2,
        ingested_at=datetime(2026, 9, 10, 8, 0, tzinfo=UTC),
        logical_date=LOGICAL_DATE,
        row_count=3,
        rejected_row_count=0,
        schema_version="1.0",
    )
    live_storage.put_object(
        BUCKET_ARCHIVE, manifest_key("fx-rates", manifest.batch_id), manifest.to_json().encode()
    )

    with DbapiTrinoExecutor(TrinoConfig.from_env()) as executor:
        result = load(live_storage, executor, spec_by_source("fx-rates"), logical_date=LOGICAL_DATE)

    assert result.row_count == 3
    where = f"where \"_batch_date\" = DATE '{LOGICAL_DATE:%Y-%m-%d}'"
    assert (
        trino_scalar(
            f'select count(distinct "_source_object") from iceberg.bronze.fx_rates {where}'
        )
        == 2
    )


def test_dbt_staging_view_builds_from_bronze(
    live_storage: BotoObjectStorage, clean_bronze: None
) -> None:
    seed_orders(live_storage, rows=3)
    with DbapiTrinoExecutor(TrinoConfig.from_env()) as executor:
        load(live_storage, executor, spec_by_source("orders"), logical_date=LOGICAL_DATE)

    subprocess.run(
        [
            "uv",
            "run",
            "dbt",
            "run",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
            "--select",
            "stg_orders",
        ],
        cwd=REPO_ROOT,
        check=True,
        env={**os.environ, "TRINO_HOST": os.environ.get("TRINO_HOST", "127.0.0.1")},
    )

    assert trino_scalar("select count(*) from iceberg.silver.stg_orders") == 3
```

Notes:
- `live_storage` fixture already exists in `tests/integration/conftest.py`.
- `dbt run` creates schema `silver` automatically (dbt creates schemas it writes to).
- If the trino client needs `no_proxy` handling in your environment, `make integration` already sets `no_proxy`/`NO_PROXY` for 127.0.0.1.

- [ ] **Step 2: Add Makefile targets** (same pattern as existing dbt commands; update `.PHONY`)

```make
dbt-build: ## Run dbt models + tests against the live core stack. ARGS="--select staging"
	@bash -c 'set -a; source .env; set +a; $(UV) run dbt build --project-dir dbt --profiles-dir dbt $(ARGS)'

dbt-test: ## Run dbt tests. ARGS="--select staging"
	@bash -c 'set -a; source .env; set +a; $(UV) run dbt test --project-dir dbt --profiles-dir dbt $(ARGS)'
```

- [ ] **Step 3: Validate against the live stack**

```bash
make up
make test          # unit tests still green, integration skipped without the gate
make integration   # runs the new live tests (requires OMNI_INTEGRATION=1, set by the target)
```

Expected: 3 new integration tests PASS. If the stack cannot run in the current environment, report exactly why (AGENTS §5.3) and leave the target listed for manual verification.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/test_bronze_load.py Makefile
git commit -m "test(integration): live Bronze load, idempotent re-run, dbt staging (Phase 5 slice 1)"
```

---

### Task 10: README update and final validation

**Files:**
- Modify: `README.md`
- Possibly fix fallout discovered by validation (no unrelated refactoring).

**Interfaces:** none (documentation only).

- [ ] **Step 1: Update README**

- In the `## Status` section change the last line to:

```markdown
- [ ] Phase 5 — dbt + Trino lakehouse (in progress: slice 1 — Bronze loader, dbt skeleton)
```

- In the `## Commands` block add:

```bash
make bronze-load # load raw archive data into Iceberg Bronze (ARGS="run --source orders --date 2026-09-18" / "run-all --date ...")
make dbt-parse   # offline dbt manifest check
make dbt-build   # run dbt models + tests against the live stack (ARGS="--select staging")
make dbt-test    # run dbt tests
```

- Add a short "Bronze layer" paragraph to the implementation narrative (after the API/architecture paragraphs): raw archive objects (PG snapshot Parquet, API JSON pages) are loaded into Iceberg `bronze.*` tables partitioned by `_batch_date`; loads are idempotent per (source, logical date) — `DELETE` partition + batched `INSERT`, verified against the raw manifest row count; empty days are warnings, schema drift and row-count mismatches fail before touching the partition.

- [ ] **Step 2: Full validation suite**

```bash
make lint
make test
make dbt-parse
cp .env.example .env && docker compose config --quiet && rm .env   # if a local .env already exists, skip the copy
make up
make integration
make smoke-core
```

Expected: all PASS. `make smoke-core` is unchanged but re-run to prove the stack is healthy after the dependency additions. If any live check cannot run, state why explicitly in the completion report — do not claim it passed.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: document Phase 5 slice 1 (Bronze loader, dbt skeleton)"
```

- [ ] **Step 4: Completion report**

Produce the AGENTS.md §49 report: Implemented / Changed files / Validation (commands + PASS/FAIL, honest) / Manual verification / Known limitations / Follow-up (slice 2: staging API + intermediate + core Kimball + tests + data-model docs).

---

## Self-Review Notes (resolved during planning)

- **Spec coverage (slice 1 scope from §10):** deps (Task 1), dbt skeleton + staging OLTP + sources.yml (Task 8), `lakehouse/bronze` module (Tasks 2–7), unit tests (Tasks 2–7), Make targets `bronze-load`/`dbt-parse`/`dbt-build`/`dbt-test` (Tasks 7–9), CI dbt-parse (Task 8), guard tests + `.env.example` (Tasks 1, 8), integration test bronze + idempotency + dbt run staging (Task 9). All §4 mechanics covered: service columns, `_batch_date` partitioning, readers with drift errors, manifest row-count check, empty-batch no-op, batched INSERT ~500, CLI run/run-all, structured logs.
- **Deviations from spec, pre-approved in plan:** CI runs `dbt parse` only (offline-safe); `dbt compile` requires a live connection (relation cache), so compile coverage comes from the live integration `dbt run`. dbt-trino pin verified resolvable (1.9.3 allows dbt-core ≥1.8).
- **Type consistency:** `Row` is `dict[str, object]` in readers and `Mapping[str, object]` in literals (consumer-side widening, fine); `LoadResult(source, batch_id, logical_date, row_count, status)` used by CLI tests via a real constructor.
