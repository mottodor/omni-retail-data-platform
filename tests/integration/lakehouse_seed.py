"""Deterministic two-day raw world for live lakehouse integration tests.

Shared seeding helpers extracted for the slice-3 orchestration test (the
first consumer). The world mirrors ``test_dbt_core_build.py``'s fixtures —
that file keeps its own inline copy for now (scope control; see the task-3
plan in git history).

Contract of the seeded world:

- DAY_1 carries full, mutually consistent snapshots of the seven OLTP
  tables plus one page per API source; DAY_2 adds the late/changed day
  (one mutated customer, one re-snapshotted order, fresh API feeds);
- raw objects land in the archive bucket together with their manifests,
  exactly as the Phase 3/4 ingestion pipelines write them;
- ``reset_bronze()`` wipes every partition of every Bronze table (the test
  owns Bronze while it runs; Bronze is rebuildable by design);
- ``purge_test_raw()`` removes only the seeded days' objects and manifests,
  never data of other local days.
"""

import hashlib
import json
from datetime import UTC, date, datetime, time
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
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
from omni_retail.lakehouse.bronze.loader import TrinoConfig
from omni_retail.lakehouse.bronze.specs import TABLES as BRONZE_TABLES

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
    """Fresh-stack guard: a Bronze table exists only after its first load."""
    count = trino_scalar(
        "select count(*) from iceberg.information_schema.tables "
        f"where table_schema = 'bronze' and table_name = '{table}'"
    )
    return bool(count == 1)


def reset_bronze() -> None:
    """Delete every partition of every Bronze table (the test owns Bronze)."""
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


def put_parquet(
    storage: BotoObjectStorage, table: str, logical_date: date, rows: list[tuple[object, ...]]
) -> None:
    """Archive one OLTP-table snapshot as Parquet plus its manifest."""
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
    """Archive one API source's pages plus the aggregate-batch manifest."""
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
    """Consistent full snapshot day: 2 categories, 2 products, 2 customers,
    2 orders (1 delivered USD, 1 refunded EUR), 3 order items, 2 payments,
    1 shipment, one page per API source."""
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
    """Late/changed day: one mutated customer (SCD2 leg), one re-snapshotted
    order (dedup leg), fresh daily API feeds (latest-wins legs)."""
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
