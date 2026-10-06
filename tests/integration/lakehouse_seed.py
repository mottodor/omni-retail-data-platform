"""Deterministic two-day raw world for live lakehouse integration tests.

Every write goes through the caller's object mutation journal. Seed functions
return their manifests so callers can construct a manifest-scoped archive view
that hides every unrelated date on a long-lived stack.
"""

# pyright: reportAttributeAccessIssue=false, reportMissingImports=false, reportMissingTypeStubs=false

import hashlib
import json
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import trino

from integration.namespace_ownership import ObjectMutationJournal
from omni_retail.ingestion.common.manifest import BatchManifest
from omni_retail.ingestion.common.paths import (
    BUCKET_ARCHIVE,
    api_batch_prefix,
    api_page_key,
    manifest_key,
    postgres_snapshot_key,
)
from omni_retail.ingestion.common.storage import ObjectStorage
from omni_retail.ingestion.postgres_snapshot.tables import (
    table_by_name,
)
from omni_retail.lakehouse.bronze.loader import DbapiTrinoExecutor, TrinoConfig
from omni_retail.streaming.cdc.consumer import CdcBatchWriter
from omni_retail.streaming.cdc.model import CdcEvent, event_identity

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
CDC_BASE_TIME = datetime(2026, 9, 22, 12, 0, tzinfo=UTC)
CDC_KEY_FIELDS = {
    "customers": "customer_id",
    "orders": "order_id",
    "payments": "payment_id",
}


def _cdc_event(
    *,
    table: str,
    business_key: int,
    operation: str,
    offset: int,
    source_lsn: int | None,
    after: dict[str, object] | None,
    source_timestamp: datetime,
) -> CdcEvent:
    """Build one deterministic raw CDC fixture with production-shaped JSON."""
    topic = f"omni.oltp.public.{table}"
    key_field = CDC_KEY_FIELDS[table]
    key = {key_field: business_key}
    before = key if operation == "d" else None
    envelope = {
        "before": before,
        "after": after,
        "source": {
            "schema": "public",
            "table": table,
            "lsn": source_lsn,
            "txId": source_lsn,
            "ts_us": int(source_timestamp.timestamp() * 1_000_000),
        },
        "op": operation,
    }
    kafka_timestamp = source_timestamp + timedelta(seconds=1)
    return CdcEvent(
        event_id=event_identity(topic, 0, offset),
        kafka_topic=topic,
        kafka_partition=0,
        kafka_offset=offset,
        kafka_timestamp=kafka_timestamp,
        source_schema="public",
        source_table=table,
        operation=operation,
        source_lsn=source_lsn,
        source_tx_id=source_lsn,
        source_timestamp=source_timestamp,
        key_json=json.dumps(key, separators=(",", ":"), sort_keys=True),
        envelope_json=json.dumps(envelope, separators=(",", ":"), sort_keys=True),
        before_json=(
            json.dumps(before, separators=(",", ":"), sort_keys=True)
            if before is not None
            else None
        ),
        after_json=(
            json.dumps(after, separators=(",", ":"), sort_keys=True) if after is not None else None
        ),
        event_date=source_timestamp.date(),
        ingested_at=CDC_BASE_TIME + timedelta(minutes=offset),
    )


def _customer_row(
    customer_id: int,
    *,
    segment: str,
    updated_at: datetime,
    region: str = "cdc-region",
) -> dict[str, object]:
    return {
        "customer_id": customer_id,
        "email": f"cdc-{customer_id}@example.test",
        "first_name": "CDC",
        "last_name": "Fixture",
        "region": region,
        "city": "cdc-city",
        "status": "active",
        "segment": segment,
        "registered_at": "2026-09-01T08:00:00.000000Z",
        "created_at": "2026-09-01T08:00:00.000000Z",
        "updated_at": updated_at.isoformat().replace("+00:00", "Z"),
    }


def _order_row(
    order_id: int,
    *,
    customer_id: int,
    status: str,
    updated_at: datetime,
) -> dict[str, object]:
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "status": status,
        "currency": "USD",
        "shipping_cost": "2.50",
        "order_total": "42.50",
        "created_at": "2026-09-20T10:00:00.000000Z",
        "updated_at": updated_at.isoformat().replace("+00:00", "Z"),
    }


def _payment_row(
    payment_id: int,
    *,
    order_id: int,
    status: str,
    updated_at: datetime,
) -> dict[str, object]:
    return {
        "payment_id": payment_id,
        "order_id": order_id,
        "method": "card",
        "status": status,
        "amount": "42.50",
        "transaction_id": f"CDC-TXN-{payment_id}",
        "created_at": "2026-09-20T10:01:00.000000Z",
        "updated_at": updated_at.isoformat().replace("+00:00", "Z"),
    }


def seed_cdc_events(executor: DbapiTrinoExecutor, *, schema: str) -> tuple[CdcEvent, ...]:
    """Seed replay, delete, recreate, same-LSN, and late-time CDC cases."""
    earlier = CDC_BASE_TIME - timedelta(days=2)
    latest_by_lsn_but_oldest_time = CDC_BASE_TIME - timedelta(days=4)
    events = (
        _cdc_event(
            table="customers",
            business_key=9_910_001,
            operation="u",
            offset=3,
            source_lsn=200,
            after={
                **_customer_row(9_910_001, segment="vip", updated_at=latest_by_lsn_but_oldest_time),
                "additive_note": "ignored-by-typed-projection",
            },
            source_timestamp=latest_by_lsn_but_oldest_time,
        ),
        _cdc_event(
            table="customers",
            business_key=9_910_001,
            operation="r",
            offset=0,
            source_lsn=None,
            after=_customer_row(9_910_001, segment="standard", updated_at=CDC_BASE_TIME),
            source_timestamp=CDC_BASE_TIME,
        ),
        _cdc_event(
            table="customers",
            business_key=9_910_001,
            operation="u",
            offset=2,
            source_lsn=200,
            after=_customer_row(9_910_001, segment="premium", updated_at=earlier),
            source_timestamp=earlier,
        ),
        _cdc_event(
            table="customers",
            business_key=9_910_002,
            operation="r",
            offset=1,
            source_lsn=None,
            after=_customer_row(9_910_002, segment="standard", updated_at=CDC_BASE_TIME),
            source_timestamp=CDC_BASE_TIME,
        ),
        _cdc_event(
            table="customers",
            business_key=9_910_002,
            operation="d",
            offset=4,
            source_lsn=201,
            after=None,
            source_timestamp=CDC_BASE_TIME + timedelta(minutes=1),
        ),
        _cdc_event(
            table="customers",
            business_key=9_910_003,
            operation="c",
            offset=5,
            source_lsn=202,
            after=_customer_row(9_910_003, segment="standard", updated_at=CDC_BASE_TIME),
            source_timestamp=CDC_BASE_TIME,
        ),
        _cdc_event(
            table="customers",
            business_key=9_910_003,
            operation="d",
            offset=6,
            source_lsn=203,
            after=None,
            source_timestamp=CDC_BASE_TIME + timedelta(minutes=2),
        ),
        _cdc_event(
            table="customers",
            business_key=9_910_003,
            operation="c",
            offset=7,
            source_lsn=204,
            after=_customer_row(
                9_910_003,
                segment="premium",
                updated_at=CDC_BASE_TIME + timedelta(minutes=3),
                region="cdc-recreated",
            ),
            source_timestamp=CDC_BASE_TIME + timedelta(minutes=3),
        ),
        _cdc_event(
            table="customers",
            business_key=990_001,
            operation="r",
            offset=8,
            source_lsn=9_999,
            after={
                **_customer_row(
                    990_001,
                    segment="premium",
                    updated_at=datetime(2026, 9, 20, 9, 0, tzinfo=UTC),
                    region="it-region",
                ),
                "email": "it-customer@example.com",
            },
            source_timestamp=datetime(2026, 9, 20, 9, 0, tzinfo=UTC),
        ),
        _cdc_event(
            table="customers",
            business_key=990_002,
            operation="r",
            offset=9,
            source_lsn=None,
            after={
                **_customer_row(
                    990_002,
                    segment="standard",
                    updated_at=datetime(2026, 9, 20, 9, 0, tzinfo=UTC),
                    region="it-region-b",
                ),
                "email": "it-customer2@example.com",
            },
            source_timestamp=datetime(2026, 9, 20, 9, 0, tzinfo=UTC),
        ),
        _cdc_event(
            table="customers",
            business_key=990_001,
            operation="u",
            offset=10,
            source_lsn=205,
            after={
                **_customer_row(
                    990_001,
                    segment="vip",
                    updated_at=datetime(2026, 9, 21, 9, 0, tzinfo=UTC),
                    region="it-region-2",
                ),
                "email": "it-customer@example.com",
            },
            source_timestamp=datetime(2026, 9, 21, 9, 0, tzinfo=UTC),
        ),
        _cdc_event(
            table="customers",
            business_key=9_910_001,
            operation="u",
            offset=11,
            source_lsn=250,
            after=_customer_row(
                9_910_001,
                segment="standard",
                updated_at=CDC_BASE_TIME + timedelta(minutes=6),
                region="cdc-later-version",
            ),
            source_timestamp=CDC_BASE_TIME + timedelta(minutes=6),
        ),
        _cdc_event(
            table="orders",
            business_key=9_920_001,
            operation="c",
            offset=0,
            source_lsn=200,
            after=_order_row(
                9_920_001,
                customer_id=9_910_001,
                status="pending",
                updated_at=CDC_BASE_TIME,
            ),
            source_timestamp=CDC_BASE_TIME,
        ),
        _cdc_event(
            table="orders",
            business_key=9_920_001,
            operation="u",
            offset=1,
            source_lsn=301,
            after=_order_row(
                9_920_001,
                customer_id=9_910_001,
                status="paid",
                updated_at=earlier,
            ),
            source_timestamp=earlier,
        ),
        _cdc_event(
            table="orders",
            business_key=9_920_002,
            operation="c",
            offset=2,
            source_lsn=302,
            after=_order_row(
                9_920_002,
                customer_id=9_910_001,
                status="pending",
                updated_at=CDC_BASE_TIME,
            ),
            source_timestamp=CDC_BASE_TIME,
        ),
        _cdc_event(
            table="orders",
            business_key=9_920_002,
            operation="d",
            offset=3,
            source_lsn=303,
            after=None,
            source_timestamp=CDC_BASE_TIME + timedelta(minutes=4),
        ),
        _cdc_event(
            table="orders",
            business_key=990_001,
            operation="r",
            offset=4,
            source_lsn=9_999,
            after={
                **_order_row(
                    990_001,
                    customer_id=990_001,
                    status="delivered",
                    updated_at=datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
                ),
                "shipping_cost": "3.00",
                "order_total": "93.00",
            },
            source_timestamp=datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
        ),
        _cdc_event(
            table="orders",
            business_key=990_002,
            operation="r",
            offset=5,
            source_lsn=None,
            after={
                **_order_row(
                    990_002,
                    customer_id=990_002,
                    status="refunded",
                    updated_at=datetime(2026, 9, 21, 10, 0, tzinfo=UTC),
                ),
                "currency": "EUR",
                "shipping_cost": "2.00",
                "order_total": "42.00",
                "created_at": "2026-09-20T11:00:00.000000Z",
            },
            source_timestamp=datetime(2026, 9, 21, 10, 0, tzinfo=UTC),
        ),
        _cdc_event(
            table="orders",
            business_key=990_002,
            operation="d",
            offset=6,
            source_lsn=303,
            after=None,
            source_timestamp=CDC_BASE_TIME + timedelta(minutes=4),
        ),
        _cdc_event(
            table="payments",
            business_key=9_930_001,
            operation="c",
            offset=0,
            source_lsn=200,
            after=_payment_row(
                9_930_001,
                order_id=9_920_001,
                status="pending",
                updated_at=CDC_BASE_TIME,
            ),
            source_timestamp=CDC_BASE_TIME,
        ),
        _cdc_event(
            table="payments",
            business_key=9_930_001,
            operation="u",
            offset=1,
            source_lsn=301,
            after=_payment_row(
                9_930_001,
                order_id=9_920_001,
                status="captured",
                updated_at=earlier,
            ),
            source_timestamp=earlier,
        ),
        _cdc_event(
            table="payments",
            business_key=9_930_002,
            operation="c",
            offset=2,
            source_lsn=302,
            after=_payment_row(
                9_930_002,
                order_id=9_920_002,
                status="pending",
                updated_at=CDC_BASE_TIME,
            ),
            source_timestamp=CDC_BASE_TIME,
        ),
        _cdc_event(
            table="payments",
            business_key=9_930_002,
            operation="d",
            offset=3,
            source_lsn=303,
            after=None,
            source_timestamp=CDC_BASE_TIME + timedelta(minutes=5),
        ),
        _cdc_event(
            table="payments",
            business_key=980_001,
            operation="r",
            offset=4,
            source_lsn=9_999,
            after={
                **_payment_row(
                    980_001,
                    order_id=990_001,
                    status="captured",
                    updated_at=datetime(2026, 9, 20, 10, 5, tzinfo=UTC),
                ),
                "amount": "93.00",
                "transaction_id": "TXN-IT-0001",
            },
            source_timestamp=datetime(2026, 9, 20, 10, 5, tzinfo=UTC),
        ),
        _cdc_event(
            table="payments",
            business_key=980_002,
            operation="r",
            offset=5,
            source_lsn=None,
            after={
                **_payment_row(
                    980_002,
                    order_id=990_002,
                    status="refunded",
                    updated_at=datetime(2026, 9, 20, 11, 5, tzinfo=UTC),
                ),
                "method": "paypal",
                "amount": "42.00",
                "transaction_id": "TXN-IT-0002",
                "created_at": "2026-09-20T11:05:00.000000Z",
            },
            source_timestamp=datetime(2026, 9, 20, 11, 5, tzinfo=UTC),
        ),
        _cdc_event(
            table="payments",
            business_key=980_002,
            operation="d",
            offset=6,
            source_lsn=303,
            after=None,
            source_timestamp=CDC_BASE_TIME + timedelta(minutes=5),
        ),
    )
    writer = CdcBatchWriter(executor, catalog="iceberg", schema=schema)
    writer.ensure_table()
    writer.write(events)
    writer.write((events[0],))  # exact transport replay must remain a no-op
    return events


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


def isolate_seed_coordinates(journal: ObjectMutationJournal) -> None:
    """Clear only deterministic seed batches after leasing their prior bytes."""
    direct_refs: set[tuple[str, str]] = set()
    postgres_days = {
        DAY_1: OLTP_TABLES,
        DAY_2: ("customers", "orders"),
    }
    for logical_date, tables in postgres_days.items():
        for table in tables:
            source = f"postgres-{table}"
            batch_id = f"{source}-{logical_date:%Y%m%d}"
            direct_refs.add((BUCKET_ARCHIVE, postgres_snapshot_key(table, logical_date)))
            direct_refs.add((BUCKET_ARCHIVE, manifest_key(source, batch_id)))
    journal.lease_keys(direct_refs)
    for bucket, key in direct_refs:
        journal.delete_object(bucket, key)

    for logical_date in (DAY_1, DAY_2):
        for source in API_SOURCES:
            prefix = api_batch_prefix(source, logical_date)
            journal.lease_prefix(BUCKET_ARCHIVE, prefix)
            for key in journal.list_object_keys(BUCKET_ARCHIVE, prefix):
                journal.delete_object(BUCKET_ARCHIVE, key)
            batch_id = f"{source}-{logical_date:%Y%m%d}"
            manifest_ref = (BUCKET_ARCHIVE, manifest_key(source, batch_id))
            journal.lease_key(*manifest_ref)
            journal.delete_object(*manifest_ref)


def read_seed_manifests(
    storage: ObjectStorage,
    logical_date: date,
    sources: tuple[str, ...],
) -> tuple[BatchManifest, ...]:
    """Read back exactly the manifests produced by one seed helper."""
    manifests: list[BatchManifest] = []
    for source in sources:
        batch_id = f"{source}-{logical_date:%Y%m%d}"
        body = storage.get_object(BUCKET_ARCHIVE, manifest_key(source, batch_id))
        manifests.append(BatchManifest.from_json(body.decode()))
    return tuple(manifests)


def put_parquet(
    storage: ObjectStorage, table: str, logical_date: date, rows: list[tuple[object, ...]]
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
    storage: ObjectStorage,
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
    storage: ObjectStorage,
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


def seed_day_1(storage: ObjectStorage) -> tuple[BatchManifest, ...]:
    """Consistent full snapshot day: 2 categories, 2 products, 2 customers,
    2 orders (1 delivered USD, 1 refunded EUR), 3 order items, 2 payments,
    2 shipments, one page per API source."""
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
            (
                970002,
                990002,
                "UPS",
                "TRK-IT-0002",
                "cancelled",
                None,
                None,
                datetime(2026, 9, 20, 11, 30, tzinfo=UTC),
                datetime(2026, 9, 20, 13, 0, tzinfo=UTC),
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
    return read_seed_manifests(
        storage,
        DAY_1,
        tuple(f"postgres-{table}" for table in OLTP_TABLES) + API_SOURCES,
    )


def seed_day_2(storage: ObjectStorage) -> tuple[BatchManifest, ...]:
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
    return read_seed_manifests(
        storage,
        DAY_2,
        ("postgres-customers", "postgres-orders", *API_SOURCES),
    )
