"""Unit tests for the Bronze loader: DDL, ordering, idempotency (fakes only)."""

import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fakes.storage import FakeStorage
from fakes.trino import FakeTrinoExecutor
from omni_retail.ingestion.common.manifest import BatchManifest
from omni_retail.ingestion.common.paths import (
    BUCKET_ARCHIVE,
    api_page_key,
    manifest_key,
    postgres_snapshot_key,
)
from omni_retail.ingestion.postgres_snapshot.tables import table_by_name
from omni_retail.lakehouse.bronze.loader import (
    DbapiTrinoExecutor,
    LoadError,
    TrinoConfig,
    create_schema_sql,
    create_table_sql,
    delete_partition_sql,
    load,
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
    storage.put_object(BUCKET_ARCHIVE, manifest_key(source, batch_id), manifest.to_json().encode())


def seed_orders_batch(storage: FakeStorage, *, rows: int, manifest_rows: int | None = None) -> None:
    body = orders_parquet(orders_rows(rows))
    storage.put_object(BUCKET_ARCHIVE, postgres_snapshot_key("orders", LOGICAL_DATE), body)
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
            BUCKET_ARCHIVE,
            api_page_key("fx-rates", LOGICAL_DATE, number),
            json.dumps(page).encode(),
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
        BUCKET_ARCHIVE,
        postgres_snapshot_key("orders", LOGICAL_DATE),
        orders_parquet(orders_rows(2)),
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
