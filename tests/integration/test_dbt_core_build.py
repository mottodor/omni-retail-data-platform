"""Live integration: full dbt build over a seeded, consistent Bronze world.

Requires the core profile (MinIO, Polaris, Trino) and OMNI_INTEGRATION=1
(``make up`` then ``make integration``). The test OWNS the bronze content
while it runs: existing partitions of the ten bronze tables are purged
first (bronze is rebuildable by design — re-run ``make bronze-load`` to
restore local data), a deterministic two-day dataset is loaded, full
``dbt build`` (models + built-in + singular tests) must succeed, and
SCD2 / point-in-time / Kimball semantics are asserted on marker entities.
"""

import hashlib
import json
import os
import subprocess
from collections.abc import Generator
from datetime import UTC, date, datetime, time
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
from omni_retail.lakehouse.bronze.specs import TABLES as BRONZE_TABLES
from omni_retail.lakehouse.bronze.specs import all_sources, spec_by_source

REPO_ROOT = Path(__file__).resolve().parents[2]
DAY_1 = date(2026, 9, 20)
DAY_2 = date(2026, 9, 21)
OLTP_TABLES = (
    "categories",
    "products",
    "customers",
    "orders",
    "order_items",
    "payments",
    "shipments",
)
API_SOURCES = ("fx-rates", "marketing-campaigns", "deliveries")


def trino_scalar(sql: str) -> object:
    """First column of the first row; None for DDL/no-row statements."""
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
    count = trino_scalar(
        "select count(*) from iceberg.information_schema.tables "
        f"where table_schema = 'bronze' and table_name = '{table}'"
    )
    return bool(count == 1)


def reset_bronze() -> None:
    """Delete every partition of every bronze table (test owns bronze)."""
    for name in BRONZE_TABLES:
        if bronze_table_exists(name):
            trino_scalar(f"delete from iceberg.bronze.{name}")


def purge_test_raw(storage: BotoObjectStorage) -> None:
    """Remove only the seeded raw objects and manifests of both days."""
    prefixes = [
        f"postgres/{name}/{day:%Y/%m/%d}/" for name in OLTP_TABLES for day in (DAY_1, DAY_2)
    ]
    prefixes += [f"api/{source}/{day:%Y%m%d}/" for source in API_SOURCES for day in (DAY_1, DAY_2)]
    prefixes += [
        f"_manifests/postgres-{name}/postgres-{name}-{day:%Y%m%d}.json"
        for name in OLTP_TABLES
        for day in (DAY_1, DAY_2)
    ]
    prefixes += [
        f"_manifests/{source}/{source}-{day:%Y%m%d}.json"
        for source in API_SOURCES
        for day in (DAY_1, DAY_2)
    ]
    for prefix in prefixes:
        for key in storage.list_object_keys(BUCKET_ARCHIVE, prefix):
            storage.delete_object(BUCKET_ARCHIVE, key)


@pytest.fixture()
def seeded_world(live_storage: BotoObjectStorage) -> Generator[date, None, None]:
    reset_bronze()
    purge_test_raw(live_storage)
    seed_day_1(live_storage)
    seed_day_2(live_storage)
    with DbapiTrinoExecutor(TrinoConfig.from_env()) as executor:
        for logical_date in (DAY_1, DAY_2):
            for source in all_sources():
                load(live_storage, executor, spec_by_source(source), logical_date=logical_date)
    yield DAY_1
    for name in BRONZE_TABLES:
        if bronze_table_exists(name):
            partition_dates = (DAY_1, DAY_2)
            for day in partition_dates:
                trino_scalar(
                    f'delete from iceberg.bronze.{name} where "_batch_date" = '
                    f"DATE '{day:%Y-%m-%d}'"
                )
    purge_test_raw(live_storage)


def put_parquet(
    storage: BotoObjectStorage, table: str, logical_date: date, rows: list[tuple[object, ...]]
) -> None:
    spec = table_by_name(table)
    arrays = [
        pa.array([row[index] for row in rows], type=column.type)
        for index, column in enumerate(spec.columns)
    ]
    arrow = pa.Table.from_arrays(arrays, schema=spec.arrow_schema())
    sink = pa.BufferOutputStream()
    pq.write_table(arrow, sink)
    body = sink.getvalue().to_pybytes()
    storage.put_object(BUCKET_ARCHIVE, postgres_snapshot_key(table, logical_date), body)
    put_postgres_manifest(
        storage,
        table=table,
        logical_date=logical_date,
        row_count=len(rows),
        object_key=postgres_snapshot_key(table, logical_date),
        checksum=hashlib.sha256(body).hexdigest(),
        size_bytes=len(body),
    )


def put_postgres_manifest(
    storage: BotoObjectStorage,
    *,
    table: str,
    logical_date: date,
    row_count: int,
    object_key: str,
    checksum: str,
    size_bytes: int,
) -> None:
    source = f"postgres-{table}"
    batch_id = f"{source}-{logical_date:%Y%m%d}"
    manifest = BatchManifest(
        batch_id=batch_id,
        source=source,
        source_kind="postgres",
        status="completed",
        object_key=object_key,
        checksum=checksum,
        size_bytes=size_bytes,
        ingested_at=datetime.combine(logical_date, time(8, 0), tzinfo=UTC),
        logical_date=logical_date,
        row_count=row_count,
        rejected_row_count=0,
        schema_version="1.0",
    )
    storage.put_object(BUCKET_ARCHIVE, manifest_key(source, batch_id), manifest.to_json().encode())


def put_api_pages(
    storage: BotoObjectStorage,
    *,
    source: str,
    logical_date: date,
    pages: list[dict[str, object]],
    row_count: int,
) -> None:
    checksum = hashlib.sha256()
    size = 0
    for number, payload in enumerate(pages, start=1):
        body = json.dumps(payload).encode()
        checksum.update(body)
        size += len(body)
        storage.put_object(BUCKET_ARCHIVE, api_page_key(source, logical_date, number), body)
    batch_id = f"{source}-{logical_date:%Y%m%d}"
    manifest = BatchManifest(
        batch_id=batch_id,
        source=source,
        source_kind="api",
        status="completed",
        object_key=f"api/{source}/{logical_date:%Y%m%d}/",
        checksum=checksum.hexdigest(),
        size_bytes=size,
        ingested_at=datetime.combine(logical_date, time(8, 0), tzinfo=UTC),
        logical_date=logical_date,
        row_count=row_count,
        rejected_row_count=0,
        schema_version="1.0",
    )
    storage.put_object(BUCKET_ARCHIVE, manifest_key(source, batch_id), manifest.to_json().encode())


def seed_day_1(storage: BotoObjectStorage) -> None:
    cat_created = datetime(2026, 9, 1, tzinfo=UTC)
    put_parquet(
        storage,
        "categories",
        DAY_1,
        [
            (90001, "it-root-cat", None, cat_created, cat_created),
            (90002, "it-leaf-cat", 90001, cat_created, cat_created),
        ],
    )
    put_parquet(
        storage,
        "products",
        DAY_1,
        [
            (
                910001,
                "IT-SKU-0001",
                "it-product-a",
                90002,
                "it-brand",
                Decimal("25.00"),
                Decimal("15.00"),
                True,
                cat_created,
                cat_created,
            ),
            (
                910002,
                "IT-SKU-0002",
                "it-product-b",
                90001,
                "it-brand",
                Decimal("40.00"),
                Decimal("22.00"),
                True,
                cat_created,
                cat_created,
            ),
        ],
    )
    put_parquet(
        storage,
        "customers",
        DAY_1,
        [
            (
                990001,
                "it-customer@example.com",
                "Ita",
                "One",
                "it-region",
                "it-city",
                "active",
                "premium",
                datetime(2026, 8, 1, tzinfo=UTC),
                datetime(2026, 8, 1, tzinfo=UTC),
                datetime(2026, 9, 20, 9, 0, tzinfo=UTC),
            ),
            (
                990002,
                "it-customer2@example.com",
                "Itb",
                "Two",
                "it-region-b",
                "it-city-b",
                "active",
                "standard",
                datetime(2026, 8, 2, tzinfo=UTC),
                datetime(2026, 8, 2, tzinfo=UTC),
                datetime(2026, 9, 20, 9, 0, tzinfo=UTC),
            ),
        ],
    )
    put_parquet(
        storage,
        "orders",
        DAY_1,
        [
            (
                990001,
                990001,
                "delivered",
                "USD",
                Decimal("3.00"),
                Decimal("93.00"),
                datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
                datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
            ),
            (
                990002,
                990002,
                "refunded",
                "EUR",
                Decimal("2.00"),
                Decimal("42.00"),
                datetime(2026, 9, 20, 11, 0, tzinfo=UTC),
                datetime(2026, 9, 20, 13, 0, tzinfo=UTC),
            ),
        ],
    )
    put_parquet(
        storage,
        "order_items",
        DAY_1,
        [
            (
                9000001,
                990001,
                910001,
                2,
                Decimal("25.00"),
                Decimal("50.00"),
                datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
                datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
            ),
            (
                9000002,
                990001,
                910002,
                1,
                Decimal("40.00"),
                Decimal("40.00"),
                datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
                datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
            ),
            (
                9000003,
                990002,
                910002,
                1,
                Decimal("40.00"),
                Decimal("40.00"),
                datetime(2026, 9, 20, 11, 0, tzinfo=UTC),
                datetime(2026, 9, 20, 11, 0, tzinfo=UTC),
            ),
        ],
    )
    put_parquet(
        storage,
        "payments",
        DAY_1,
        [
            (
                980001,
                990001,
                "card",
                "captured",
                Decimal("93.00"),
                "TXN-IT-0001",
                datetime(2026, 9, 20, 10, 5, tzinfo=UTC),
                datetime(2026, 9, 20, 10, 5, tzinfo=UTC),
            ),
            (
                980002,
                990002,
                "paypal",
                "refunded",
                Decimal("42.00"),
                "TXN-IT-0002",
                datetime(2026, 9, 20, 11, 5, tzinfo=UTC),
                datetime(2026, 9, 20, 11, 5, tzinfo=UTC),
            ),
        ],
    )
    put_parquet(
        storage,
        "shipments",
        DAY_1,
        [
            (
                970001,
                990001,
                "DHL",
                "TRK-IT-0001",
                "delivered",
                datetime(2026, 9, 20, 15, 0, tzinfo=UTC),
                datetime(2026, 9, 20, 18, 0, tzinfo=UTC),
                datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
                datetime(2026, 9, 20, 18, 0, tzinfo=UTC),
            ),
        ],
    )
    put_api_pages(
        storage,
        source="fx-rates",
        logical_date=DAY_1,
        pages=[
            {"rates": [{"currency": "USD", "rate": 1.0912}, {"currency": "GBP", "rate": 0.8501}]}
        ],
        row_count=2,
    )
    put_api_pages(
        storage,
        source="marketing-campaigns",
        logical_date=DAY_1,
        pages=[
            {
                "campaigns": [
                    {
                        "campaign_id": "CMP-IT-0001",
                        "name": "It Sale",
                        "channel": "search",
                        "status": "active",
                        "start_date": "2026-09-01",
                        "end_date": "2026-09-30",
                        "budget_eur": 10000.0,
                        "spend_eur": 2500.5,
                        "impressions": 100000,
                        "clicks": 3200,
                    }
                ]
            }
        ],
        row_count=1,
    )
    put_api_pages(
        storage,
        source="deliveries",
        logical_date=DAY_1,
        pages=[
            {
                "deliveries": [
                    {
                        "delivery_id": "DLV-IT-000001",
                        "order_id": "ORD-IT-0001",
                        "carrier": "DHL",
                        "status": "in_transit",
                        "shipped_at": "2026-09-20T15:00:00+00:00",
                        "delivered_at": None,
                        "updated_at": "2026-09-20T16:00:00+00:00",
                    }
                ]
            }
        ],
        row_count=1,
    )


def seed_day_2(storage: BotoObjectStorage) -> None:
    put_parquet(
        storage,
        "customers",
        DAY_2,
        [
            (
                990001,
                "it-customer@example.com",
                "Ita",
                "One",
                "it-region-2",
                "it-city-2",
                "active",
                "premium",
                datetime(2026, 8, 1, tzinfo=UTC),
                datetime(2026, 8, 1, tzinfo=UTC),
                datetime(2026, 9, 21, 9, 0, tzinfo=UTC),
            ),
        ],
    )
    put_parquet(
        storage,
        "orders",
        DAY_2,
        [
            (
                990002,
                990002,
                "refunded",
                "EUR",
                Decimal("2.00"),
                Decimal("42.00"),
                datetime(2026, 9, 20, 11, 0, tzinfo=UTC),
                datetime(2026, 9, 21, 10, 0, tzinfo=UTC),
            ),
        ],
    )
    put_api_pages(
        storage,
        source="fx-rates",
        logical_date=DAY_2,
        pages=[{"rates": [{"currency": "USD", "rate": 1.1020}]}],
        row_count=1,
    )
    put_api_pages(
        storage,
        source="marketing-campaigns",
        logical_date=DAY_2,
        pages=[
            {
                "campaigns": [
                    {
                        "campaign_id": "CMP-IT-0001",
                        "name": "It Sale",
                        "channel": "search",
                        "status": "active",
                        "start_date": "2026-09-01",
                        "end_date": "2026-09-30",
                        "budget_eur": 10000.0,
                        "spend_eur": 2500.5,
                        "impressions": 100000,
                        "clicks": 3200,
                    }
                ]
            }
        ],
        row_count=1,
    )
    put_api_pages(
        storage,
        source="deliveries",
        logical_date=DAY_2,
        pages=[
            {
                "deliveries": [
                    {
                        "delivery_id": "DLV-IT-000001",
                        "order_id": "ORD-IT-0001",
                        "carrier": "DHL",
                        "status": "delivered",
                        "shipped_at": "2026-09-20T15:00:00+00:00",
                        "delivered_at": "2026-09-21T09:30:00+00:00",
                        "updated_at": "2026-09-21T09:30:00+00:00",
                    }
                ]
            }
        ],
        row_count=1,
    )


def test_full_dbt_build_with_kimball_semantics(seeded_world: date) -> None:
    completed = subprocess.run(
        [
            "uv",
            "run",
            "dbt",
            "build",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
        ],
        cwd=REPO_ROOT,
        check=False,
        timeout=900,
        env={**os.environ, "TRINO_HOST": os.environ.get("TRINO_HOST", "127.0.0.1")},
    )
    assert completed.returncode == 0, "dbt build (models + tests) must succeed"

    # SCD2: two versions for the mutated customer, intervals contiguous.
    assert (
        trino_scalar("select count(*) from iceberg.gold.dim_customer where customer_id = 990001")
        == 2
    )
    assert (
        trino_scalar(
            "select region from iceberg.gold.dim_customer where customer_id = 990001 and is_current"
        )
        == "it-region-2"
    )
    assert trino_scalar(
        "select valid_to from iceberg.gold.dim_customer "
        "where customer_id = 990001 and not is_current"
    ) == date(2026, 9, 20)
    # Unchanged customer keeps exactly one (current) version.
    assert (
        trino_scalar("select count(*) from iceberg.gold.dim_customer where customer_id = 990002")
        == 1
    )

    # Point-in-time join: order created on DAY_1 binds the DAY_1 version.
    assert (
        trino_scalar("select customer_key from iceberg.gold.fact_orders where order_id = 990001")
        == "990001_2026-09-20"
    )

    # Kimball shapes on marker entities.
    assert (
        trino_scalar(
            "select count(*) from iceberg.gold.fact_order_items where order_id in (990001, 990002)"
        )
        == 3
    )
    assert trino_scalar("select count(*) from iceberg.gold.fact_payments") == 2
    assert trino_scalar("select count(*) from iceberg.gold.fact_shipments") == 1
    assert (
        trino_scalar("select count(*) from iceberg.gold.dim_campaign") == 1
    )  # dedup across the two daily snapshots
    assert (
        trino_scalar("select category_name from iceberg.gold.dim_product where product_id = 910002")
        == "it-root-cat"
    )
    # Latest-wins semantics for daily feeds.
    assert (
        trino_scalar(
            "select count(*) from iceberg.silver.int_fx_rates_daily where currency = 'USD'"
        )
        == 2
    )
    assert (
        trino_scalar(
            "select status from iceberg.silver.int_deliveries where delivery_id = 'DLV-IT-000001'"
        )
        == "delivered"
    )
    # Calendar dimension spans the order-activity window.
    assert trino_scalar("select min(full_date) from iceberg.gold.dim_date") == DAY_1
    assert trino_scalar("select max(full_date) from iceberg.gold.dim_date") == DAY_2
