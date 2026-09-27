"""Live integration: archive raw -> Bronze load -> idempotent re-run -> dbt staging.

Requires the core profile (MinIO, Polaris, Trino) and OMNI_INTEGRATION=1
(``make up`` then ``make integration``). Seeded raw objects are deterministic;
raw objects are purged around each test and loaded bronze partitions are
deleted afterwards (Polaris denies DROP to the bootstrap principal).
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
from omni_retail.lakehouse.bronze.loader import (
    DbapiTrinoExecutor,
    TrinoConfig,
    load,
    load_new,
)
from omni_retail.lakehouse.bronze.specs import spec_by_source

REPO_ROOT = Path(__file__).resolve().parents[2]
LOGICAL_DATE = date(2026, 9, 10)
NEXT_LOGICAL_DATE = date(2026, 9, 11)
ORDERS_SNAPSHOT = table_by_name("orders")


def trino_scalar(sql: str) -> object:
    """Return the first column of the first row, or None for DDL/no-row statements."""
    config = TrinoConfig.from_env()
    connection = trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host=config.host, port=config.port, user=config.user, catalog=config.catalog
    )
    cursor = connection.cursor()
    cursor.execute(sql)
    row = cursor.fetchone()
    connection.close()
    if row is None:
        return None
    return row[0]


def bronze_table_exists(table: str) -> bool:
    """Fresh-stack guard: a bronze table exists only after its first load."""
    count = trino_scalar(
        "select count(*) from iceberg.information_schema.tables "
        f"where table_schema = 'bronze' and table_name = '{table}'"
    )
    return bool(count == 1)


def purge_raw(storage: BotoObjectStorage) -> None:
    for prefix in (
        f"postgres/orders/{LOGICAL_DATE:%Y/%m/%d}/",
        f"postgres/orders/{NEXT_LOGICAL_DATE:%Y/%m/%d}/",
        "_manifests/postgres-orders/",
        f"api/fx-rates/{LOGICAL_DATE:%Y%m%d}/",
        "_manifests/fx-rates/",
    ):
        for key in storage.list_object_keys(BUCKET_ARCHIVE, prefix):
            storage.delete_object(BUCKET_ARCHIVE, key)


@pytest.fixture()
def clean_bronze(live_storage: BotoObjectStorage) -> Generator[None, None, None]:
    purge_raw(live_storage)
    yield
    # Teardown: remove the loaded DATA by partition (a shared local stack may
    # hold other partitions of the same bronze tables) and drop the dbt-built
    # staging view — Trino purge-drops work since the Phase 5 infra follow-up
    # (CATALOG_MANAGE_CONTENT grant + DROP_WITH_PURGE_ENABLED).
    partition = f"\"_batch_date\" = DATE '{LOGICAL_DATE:%Y-%m-%d}'"
    next_partition = f"\"_batch_date\" = DATE '{NEXT_LOGICAL_DATE:%Y-%m-%d}'"
    for table in ("orders", "fx_rates"):
        if bronze_table_exists(table):
            trino_scalar(f"delete from iceberg.bronze.{table} where {partition}")
            trino_scalar(f"delete from iceberg.bronze.{table} where {next_partition}")
    trino_scalar("drop view if exists iceberg.silver.stg_orders")
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


def seed_orders(storage: BotoObjectStorage, rows: int, logical_date: date = LOGICAL_DATE) -> None:
    data = orders_rows(rows)
    arrays = [
        pa.array([row[index] for row in data], type=column.type)
        for index, column in enumerate(ORDERS_SNAPSHOT.columns)
    ]
    table = pa.Table.from_arrays(arrays, schema=ORDERS_SNAPSHOT.arrow_schema())
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    body = sink.getvalue().to_pybytes()
    storage.put_object(BUCKET_ARCHIVE, postgres_snapshot_key("orders", logical_date), body)
    manifest = BatchManifest(
        batch_id=f"postgres-orders-{logical_date:%Y%m%d}",
        source="postgres-orders",
        source_kind="postgres",
        status="completed",
        object_key=postgres_snapshot_key("orders", logical_date),
        checksum=hashlib.sha256(body).hexdigest(),
        size_bytes=len(body),
        ingested_at=datetime(2026, 9, 10, 8, 0, tzinfo=UTC),
        logical_date=logical_date,
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


def test_bronze_load_new_loads_dates_past_watermark(
    live_storage: BotoObjectStorage, clean_bronze: None
) -> None:
    seed_orders(live_storage, rows=3, logical_date=LOGICAL_DATE)
    seed_orders(live_storage, rows=4, logical_date=NEXT_LOGICAL_DATE)

    with DbapiTrinoExecutor(TrinoConfig.from_env()) as executor:
        first_day = load(
            live_storage, executor, spec_by_source("orders"), logical_date=LOGICAL_DATE
        )
        assert first_day.row_count == 3

        results = load_new(live_storage, executor, spec_by_source("orders"))

    # only the day past the watermark was loaded, and a re-run is a no-op
    assert [result.logical_date for result in results] == [NEXT_LOGICAL_DATE]
    with DbapiTrinoExecutor(TrinoConfig.from_env()) as executor:
        assert load_new(live_storage, executor, spec_by_source("orders")) == []

    counts = {
        day: trino_scalar(
            f"select count(*) from iceberg.bronze.orders "
            f"where \"_batch_date\" = DATE '{day:%Y-%m-%d}'"
        )
        for day in (LOGICAL_DATE, NEXT_LOGICAL_DATE)
    }
    assert counts == {LOGICAL_DATE: 3, NEXT_LOGICAL_DATE: 4}
