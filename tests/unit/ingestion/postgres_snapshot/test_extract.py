"""Unit tests for keyset extraction and Parquet snapshots with fakes."""

# pyright: reportMissingImports=false, reportMissingTypeStubs=false

import hashlib
import io
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pyarrow.parquet as pq
import pytest

from fakes.postgres import FakeSnapshotConnection
from fakes.storage import FakeStorage
from omni_retail.ingestion.common.paths import (
    manifest_key,
    postgres_snapshot_key,
    postgres_watermark_key,
)
from omni_retail.ingestion.common.storage import StorageError
from omni_retail.ingestion.postgres_snapshot.extract import (
    build_select,
    extract_rows,
    rows_to_parquet,
    snapshot_table,
)
from omni_retail.ingestion.postgres_snapshot.tables import (
    UPDATED_AT,
    table_by_name,
)
from omni_retail.ingestion.postgres_snapshot.watermark import (
    Watermark,
    load_watermark,
)

ORDERS = table_by_name("orders")


def fixed_clock() -> datetime:
    return datetime(2026, 9, 13, 8, 0, tzinfo=UTC)


def orders_row(order_id: int, updated_at: datetime, total: str = "10.00") -> tuple[object, ...]:
    return (
        order_id,
        900 + order_id,
        "paid",
        "USD",
        Decimal("1.50"),
        Decimal(total),
        updated_at - timedelta(days=1),
        updated_at,
    )


def test_build_select_full_snapshot_has_no_watermark() -> None:
    sql, params = build_select(ORDERS, None)

    assert sql.startswith('select "order_id"')
    assert "where" not in sql
    assert sql.endswith('order by "updated_at", "order_id"')
    assert params == ()


def test_build_select_incremental_uses_keyset_comparison() -> None:
    watermark = Watermark("orders", datetime(2026, 9, 12, 10, 0, tzinfo=UTC), 100)

    sql, params = build_select(ORDERS, watermark)

    assert 'where ("updated_at", "order_id") > (%s, %s)' in sql
    assert sql.endswith('order by "updated_at", "order_id"')
    assert params == (watermark.updated_at, 100)


def test_extract_rows_uses_named_cursor_and_fetchmany_batches() -> None:
    rows = [orders_row(i, datetime(2026, 9, 12, tzinfo=UTC)) for i in range(5)]
    conn = FakeSnapshotConnection(rows)

    extracted = extract_rows(conn, ORDERS, None, fetch_size=2)

    assert extracted == rows
    assert conn.requested_names == ["snapshot_orders"]
    cursor = conn.cursors[0]
    assert cursor.itersize == 2
    assert cursor.executed_sql is not None and "where" not in cursor.executed_sql
    assert cursor.executed_params == ()


def test_extract_rows_passes_keyset_params_to_incremental_query() -> None:
    watermark = Watermark("orders", datetime(2026, 9, 12, 10, 0, tzinfo=UTC), 7)
    conn = FakeSnapshotConnection([])

    extract_rows(conn, ORDERS, watermark)

    cursor = conn.cursors[0]
    assert cursor.executed_params == (watermark.updated_at, 7)


def test_rows_to_parquet_roundtrips_with_explicit_schema() -> None:
    rows = [orders_row(1, datetime(2026, 9, 12, tzinfo=UTC), "19.99")]

    body = rows_to_parquet(ORDERS, rows)

    table = pq.read_table(io.BytesIO(body))
    assert table.schema.names == [column.name for column in ORDERS.columns]
    assert table.num_rows == 1
    assert table.column("order_id").to_pylist() == [1]
    assert table.column("order_total").to_pylist() == [Decimal("19.99")]
    assert table.column(UPDATED_AT).to_pylist() == [datetime(2026, 9, 12, tzinfo=UTC)]


def test_rows_to_parquet_handles_nullable_columns() -> None:
    shipments = table_by_name("shipments")
    base = datetime(2026, 9, 12, tzinfo=UTC)
    row = (
        1,  # shipment_id
        2,  # order_id
        "postal",
        "TRK-1",
        "delivered",
        base - timedelta(days=2),
        base - timedelta(days=1),
        base - timedelta(days=3),
        base,
    )

    body = rows_to_parquet(shipments, [row])

    table = pq.read_table(io.BytesIO(body))
    assert table.column("delivered_at").to_pylist() == [base - timedelta(days=1)]


def test_snapshot_table_full_run_uploads_parquet_manifest_and_watermark() -> None:
    rows = [
        orders_row(1, datetime(2026, 9, 12, 10, 0, tzinfo=UTC)),
        orders_row(2, datetime(2026, 9, 12, 11, 0, tzinfo=UTC)),
    ]
    storage = FakeStorage()

    manifest = snapshot_table(
        storage,
        FakeSnapshotConnection(rows),
        ORDERS,
        logical_date=date(2026, 9, 13),
        clock=fixed_clock,
    )

    object_key = postgres_snapshot_key("orders", date(2026, 9, 13))
    assert manifest.batch_id == "postgres-orders-20260913"
    assert manifest.source == "postgres-orders"
    assert manifest.source_kind == "postgres"
    assert manifest.status == "completed"
    assert manifest.row_count == 2
    assert manifest.logical_date == date(2026, 9, 13)
    assert manifest.size_bytes > 0
    assert (
        manifest.checksum == hashlib.sha256(storage.get_object("archive", object_key)).hexdigest()
    )
    assert storage.object_exists("archive", manifest_key("postgres-orders", manifest.batch_id))

    watermark = load_watermark(storage, ORDERS)
    assert watermark is not None
    assert watermark.updated_at == datetime(2026, 9, 12, 11, 0, tzinfo=UTC)
    assert watermark.pk == 2


def test_snapshot_table_rerun_same_day_overwrites_same_keys() -> None:
    rows = [orders_row(1, datetime(2026, 9, 12, 10, 0, tzinfo=UTC))]
    storage = FakeStorage()

    first = snapshot_table(
        storage,
        FakeSnapshotConnection(rows),
        ORDERS,
        logical_date=date(2026, 9, 13),
        clock=fixed_clock,
    )
    second = snapshot_table(
        storage,
        FakeSnapshotConnection(rows),
        ORDERS,
        logical_date=date(2026, 9, 13),
        clock=fixed_clock,
    )

    assert first.batch_id == second.batch_id
    parquet_objects = [
        key for (_bucket, key) in storage.stored_objects() if key.endswith("data.parquet")
    ]
    assert parquet_objects == [postgres_snapshot_key("orders", date(2026, 9, 13))]


def test_snapshot_table_empty_window_writes_manifest_only() -> None:
    storage = FakeStorage()

    manifest = snapshot_table(
        storage,
        FakeSnapshotConnection([]),
        ORDERS,
        logical_date=date(2026, 9, 13),
        watermark=Watermark("orders", datetime(2026, 9, 12, tzinfo=UTC), 1),
        clock=fixed_clock,
    )

    assert manifest.status == "completed"
    assert manifest.row_count == 0
    assert manifest.size_bytes == 0
    assert not storage.object_exists("archive", postgres_snapshot_key("orders", date(2026, 9, 13)))
    assert storage.object_exists("archive", manifest_key("postgres-orders", manifest.batch_id))
    assert not any(key.startswith("_watermarks/") for (_bucket, key) in storage.stored_objects())


def test_empty_rerun_removes_stale_parquet_before_zero_row_manifest() -> None:
    storage = FakeStorage()
    logical_date = date(2026, 9, 13)
    stale_key = postgres_snapshot_key("orders", logical_date)
    storage.put_object("archive", stale_key, b"foreign-stale-parquet")

    manifest = snapshot_table(
        storage,
        FakeSnapshotConnection([]),
        ORDERS,
        logical_date=logical_date,
        watermark=Watermark("orders", datetime(2026, 9, 12, tzinfo=UTC), 1),
        clock=fixed_clock,
    )

    assert manifest.row_count == 0
    assert not storage.object_exists("archive", stale_key)
    stored_manifest = storage.get_object(
        "archive", manifest_key("postgres-orders", manifest.batch_id)
    )
    assert b'"row_count": 0' in stored_manifest


def test_snapshot_table_watermark_does_not_move_when_upload_fails() -> None:
    """An interruption between upload attempts must not advance the watermark.

    The first ``put_object`` is the manifest write; failing the second one
    simulates a broken Parquet upload. The error propagates (task retry
    territory) and the watermark object stays absent.
    """

    class FailSecondPut(FakeStorage):
        def __init__(self) -> None:
            super().__init__()
            self.put_calls = 0

        def put_object(self, bucket: str, key: str, body: bytes) -> None:
            self.put_calls += 1
            if self.put_calls == 2:
                raise StorageError("simulated upload failure")
            super().put_object(bucket, key, body)

    storage = FailSecondPut()
    rows = [orders_row(1, datetime(2026, 9, 12, 10, 0, tzinfo=UTC))]

    with pytest.raises(StorageError, match="simulated upload failure"):
        snapshot_table(
            storage,
            FakeSnapshotConnection(rows),
            ORDERS,
            logical_date=date(2026, 9, 13),
            clock=fixed_clock,
        )

    assert not any(key.startswith("_watermarks/") for (_bucket, key) in storage.stored_objects())
    assert not storage.object_exists("archive", postgres_watermark_key("orders"))
