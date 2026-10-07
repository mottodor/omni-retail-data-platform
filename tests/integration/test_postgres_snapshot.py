"""Live PostgreSQL snapshot lifecycle with exact archive/watermark ownership."""

# pyright: reportMissingImports=false, reportMissingTypeStubs=false

import io
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import cast

import psycopg
import pyarrow.parquet as pq

from integration.namespace_ownership import ObjectMutationJournal, ObjectRef, ScopedObjectStorage
from omni_retail.ingestion.common.manifest import BatchManifest
from omni_retail.ingestion.common.paths import (
    BUCKET_ARCHIVE,
    manifest_key,
    postgres_snapshot_key,
    postgres_watermark_key,
)
from omni_retail.ingestion.postgres_snapshot.config import (
    PostgresSourceConfig,
)
from omni_retail.ingestion.postgres_snapshot.extract import (
    SnapshotConnection,
    snapshot_table,
)
from omni_retail.ingestion.postgres_snapshot.tables import (
    table_by_name,
)
from omni_retail.ingestion.postgres_snapshot.watermark import (
    load_watermark,
)

CUSTOMERS = table_by_name("customers")
DAY_1 = date(2026, 9, 13)
DAY_2 = date(2026, 9, 14)


def isolated_snapshot_storage(journal: ObjectMutationJournal) -> ScopedObjectStorage:
    """Lease only two snapshot batches and the exact customers watermark."""
    refs: set[ObjectRef] = {(BUCKET_ARCHIVE, postgres_watermark_key("customers"))}
    for logical_date in (DAY_1, DAY_2):
        batch_id = f"postgres-customers-{logical_date:%Y%m%d}"
        refs.add((BUCKET_ARCHIVE, postgres_snapshot_key("customers", logical_date)))
        refs.add((BUCKET_ARCHIVE, manifest_key("postgres-customers", batch_id)))
    journal.lease_keys(refs)
    storage = ScopedObjectStorage(journal, keys=refs)
    for bucket, key in refs:
        storage.delete_object(bucket, key)
    return storage


def create_isolated_customers_table(conn: psycopg.Connection) -> None:
    """Shadow public.customers with a session-local table for snapshot SQL."""
    conn.execute(
        "create temporary table customers "
        "(like public.customers including all) on commit preserve rows"
    )
    conn.execute("set search_path to pg_temp, public")
    conn.commit()


def public_marker_count(conn: psycopg.Connection, marker: str) -> int:
    row = conn.execute(
        "select count(*) from public.customers where email = %s",
        (marker,),
    ).fetchone()
    return 0 if row is None else int(cast(int, row[0]))


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


def read_parquet_rows(storage: ScopedObjectStorage, key: str) -> int:
    body = storage.get_object(BUCKET_ARCHIVE, key)
    return int(pq.read_table(io.BytesIO(body)).num_rows)


def read_manifest(storage: ScopedObjectStorage, logical_date: date) -> BatchManifest:
    batch_id = f"postgres-customers-{logical_date:%Y%m%d}"
    body = storage.get_object(BUCKET_ARCHIVE, manifest_key("postgres-customers", batch_id))
    return BatchManifest.from_json(body.decode())


def test_snapshot_lifecycle_full_empty_increment(
    object_journal: ObjectMutationJournal,
) -> None:
    storage = isolated_snapshot_storage(object_journal)
    marker = f"it-pg-snap-{uuid.uuid4().hex[:8]}@example.com"
    ts_base = datetime(2026, 9, 13, 9, 0, tzinfo=UTC)
    with psycopg.connect(PostgresSourceConfig.from_env().conninfo()) as conn:
        create_isolated_customers_table(conn)
        assert public_marker_count(conn, marker) == 0
        customer_id = insert_controlled_customer(conn, marker, ts_base)
        try:
            total = count_rows(conn)
            first = snapshot_table(
                storage, cast(SnapshotConnection, conn), CUSTOMERS, logical_date=DAY_1
            )
            assert first.status == "completed"
            assert first.row_count == total
            key_day_1 = postgres_snapshot_key("customers", DAY_1)
            assert read_parquet_rows(storage, key_day_1) == total
            watermark = load_watermark(storage, CUSTOMERS)
            assert watermark is not None
            assert watermark.updated_at == ts_base
            assert watermark.pk == customer_id

            # Empty increment uses its own deterministic date. Any stale object
            # at that coordinate must be absent when the zero-row manifest lands.
            second = snapshot_table(
                storage,
                cast(SnapshotConnection, conn),
                CUSTOMERS,
                logical_date=DAY_2,
                watermark=watermark,
            )
            assert second.row_count == 0
            key_day_2 = postgres_snapshot_key("customers", DAY_2)
            assert not storage.object_exists(BUCKET_ARCHIVE, key_day_2)
            assert read_manifest(storage, DAY_2).row_count == 0
            assert load_watermark(storage, CUSTOMERS) == watermark

            ts_mutation = ts_base + timedelta(hours=1)
            conn.execute(
                "update customers set status = 'inactive', updated_at = %s where email = %s",
                (ts_mutation, marker),
            )
            conn.commit()
            third = snapshot_table(
                storage,
                cast(SnapshotConnection, conn),
                CUSTOMERS,
                logical_date=DAY_2,
                watermark=watermark,
            )
            assert third.row_count == 1
            assert read_parquet_rows(storage, key_day_2) == 1
            advanced = load_watermark(storage, CUSTOMERS)
            assert advanced is not None
            assert advanced.updated_at == ts_mutation
            assert advanced.pk == customer_id

            assert storage.list_object_keys(BUCKET_ARCHIVE, "postgres/customers/") == (
                key_day_1,
                key_day_2,
            )
            assert storage.list_object_keys(BUCKET_ARCHIVE, "_manifests/postgres-customers/") == (
                manifest_key("postgres-customers", first.batch_id),
                manifest_key("postgres-customers", third.batch_id),
            )
            assert storage.list_object_keys(BUCKET_ARCHIVE, "_watermarks/postgres/") == (
                postgres_watermark_key("customers"),
            )

            full_refresh = snapshot_table(
                storage,
                cast(SnapshotConnection, conn),
                CUSTOMERS,
                logical_date=DAY_2,
                watermark=None,
            )
            assert full_refresh.row_count == total
            assert read_parquet_rows(storage, key_day_2) == total
        finally:
            conn.rollback()
            assert public_marker_count(conn, marker) == 0
            conn.commit()
