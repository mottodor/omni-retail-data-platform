"""Integration tests for PostgreSQL snapshot extraction (live core stack).

Runs only when ``OMNI_INTEGRATION=1`` is set (see ``make integration``) and
requires the core profile: the OLTP PostgreSQL (with the Phase 2 initial
load) and MinIO. Scenario (Phase 4 design spec §12): a controlled insert
drives a full snapshot; a re-run of the same window is empty and creates no
duplicates; a controlled mutation drives an incremental extract of exactly
one row.
"""

import io
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import cast

import psycopg
import pyarrow.parquet as pq

from omni_retail.ingestion.common.paths import (
    BUCKET_ARCHIVE,
    manifest_key,
    postgres_snapshot_key,
)
from omni_retail.ingestion.common.storage import BotoObjectStorage
from omni_retail.ingestion.postgres_snapshot.config import PostgresSourceConfig
from omni_retail.ingestion.postgres_snapshot.extract import SnapshotConnection, snapshot_table
from omni_retail.ingestion.postgres_snapshot.tables import table_by_name
from omni_retail.ingestion.postgres_snapshot.watermark import load_watermark

CUSTOMERS = table_by_name("customers")
DAY_1 = date(2026, 9, 13)
DAY_2 = date(2026, 9, 14)


def purge_customers_namespace(storage: BotoObjectStorage) -> None:
    """Delete only the customers snapshot namespace (other tables untouched)."""
    for bucket, prefix in (
        (BUCKET_ARCHIVE, "postgres/customers/"),
        (BUCKET_ARCHIVE, "_manifests/postgres-customers/"),
        (BUCKET_ARCHIVE, "_watermarks/postgres/customers.json"),
    ):
        for key in storage.list_object_keys(bucket, prefix):
            storage.delete_object(bucket, key)


def controlled_customer(marker: str, updated_at: datetime) -> tuple[object, ...]:
    return (
        marker,
        "Integration",
        "Snapshot",
        "it-region",
        "it-city",
        "active",
        "standard",
        updated_at - timedelta(days=30),
        updated_at - timedelta(days=30),
        updated_at,
    )


def insert_controlled_customer(conn: psycopg.Connection, marker: str, updated_at: datetime) -> int:
    row = controlled_customer(marker, updated_at)
    # Explicit id (max + 1): the identity sequence may lag behind explicit-id
    # loads done by the Phase 2 generator, and the test must not depend on it.
    next_id_row = conn.execute("select coalesce(max(customer_id), 0) + 1 from customers").fetchone()
    next_id = 0 if next_id_row is None else int(cast(int, next_id_row[0]))
    inserted = conn.execute(
        "insert into customers (customer_id, email, first_name, last_name, region, city, "
        "status, segment, registered_at, created_at, updated_at) "
        "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "returning customer_id",
        (next_id, *row),
    ).fetchone()
    conn.commit()
    assert inserted is not None
    return int(cast(int, inserted[0]))


def count_rows(conn: psycopg.Connection) -> int:
    row = conn.execute("select count(*) from customers").fetchone()
    conn.commit()
    return 0 if row is None else int(cast(int, row[0]))


def read_parquet_rows(storage: BotoObjectStorage, key: str) -> int:
    body = storage.get_object(BUCKET_ARCHIVE, key)
    return int(pq.read_table(io.BytesIO(body)).num_rows)


def test_snapshot_lifecycle_full_empty_increment(
    live_storage: BotoObjectStorage,
) -> None:
    purge_customers_namespace(live_storage)
    marker = f"it-pg-snap-{uuid.uuid4().hex[:8]}@example.com"
    ts_base = datetime(2026, 9, 13, 9, 0, tzinfo=UTC)
    with psycopg.connect(PostgresSourceConfig.from_env().conninfo()) as conn:
        customer_id = insert_controlled_customer(conn, marker, ts_base)
        total = count_rows(conn)
        try:
            # 1) no watermark -> full snapshot, durable parquet + manifest + watermark
            first = snapshot_table(
                live_storage, cast(SnapshotConnection, conn), CUSTOMERS, logical_date=DAY_1
            )
            assert first.status == "completed"
            assert first.row_count == total
            assert first.source_kind == "postgres"
            key_day_1 = postgres_snapshot_key("customers", DAY_1)
            assert live_storage.object_exists(BUCKET_ARCHIVE, key_day_1)
            assert read_parquet_rows(live_storage, key_day_1) == total
            assert live_storage.object_exists(
                BUCKET_ARCHIVE, manifest_key("postgres-customers", first.batch_id)
            )
            watermark = load_watermark(live_storage, CUSTOMERS)
            assert watermark is not None
            assert watermark.updated_at == ts_base
            assert watermark.pk == customer_id  # the controlled row sorts last

            # 2) re-run of the same window: empty increment, no duplicates
            second = snapshot_table(
                live_storage,
                cast(SnapshotConnection, conn),
                CUSTOMERS,
                logical_date=DAY_1,
                watermark=watermark,
            )
            assert second.status == "completed"
            assert second.row_count == 0
            assert second.batch_id == first.batch_id  # same logical day -> overwrite
            parquet_objects = live_storage.list_object_keys(BUCKET_ARCHIVE, "postgres/customers/")
            assert parquet_objects == (key_day_1,)
            manifests = live_storage.list_object_keys(
                BUCKET_ARCHIVE, "_manifests/postgres-customers/"
            )
            assert len(manifests) == 1
            assert load_watermark(live_storage, CUSTOMERS) == watermark  # unmoved

            # 3) mutation -> incremental extract of exactly the changed row
            ts_mutation = ts_base + timedelta(hours=1)
            conn.execute(
                "update customers set status = 'inactive', updated_at = %s where email = %s",
                (ts_mutation, marker),
            )
            conn.commit()
            third = snapshot_table(
                live_storage,
                cast(SnapshotConnection, conn),
                CUSTOMERS,
                logical_date=DAY_2,
                watermark=watermark,
            )
            assert third.row_count == 1
            key_day_2 = postgres_snapshot_key("customers", DAY_2)
            assert read_parquet_rows(live_storage, key_day_2) == 1
            advanced = load_watermark(live_storage, CUSTOMERS)
            assert advanced is not None
            assert advanced.updated_at == ts_mutation
            assert advanced.pk == customer_id

            # watermark objects are idempotent single-key writes
            watermark_keys = live_storage.list_object_keys(BUCKET_ARCHIVE, "_watermarks/postgres/")
            assert watermark_keys == ("_watermarks/postgres/customers.json",)

            # 4) full_refresh ignores the watermark and re-extracts everything
            full_refresh = snapshot_table(
                live_storage,
                cast(SnapshotConnection, conn),
                CUSTOMERS,
                logical_date=DAY_2,
                watermark=None,
            )
            assert full_refresh.row_count == total
            assert read_parquet_rows(live_storage, key_day_2) == total
        finally:
            # 5) cleanup: remove the controlled row, then purge the namespace
            conn.execute("delete from customers where email = %s", (marker,))
            conn.commit()
    purge_customers_namespace(live_storage)
