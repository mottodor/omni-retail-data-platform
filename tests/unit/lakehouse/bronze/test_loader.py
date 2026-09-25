"""Unit tests for the Bronze loader: DDL, ordering, idempotency (fakes only)."""

import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from trino.exceptions import TrinoQueryError

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
    discover_archive_dates,
    load,
    load_new,
    load_with_retry,
    max_batch_date_sql,
    read_watermark,
    table_exists_sql,
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


def write_manifest(
    storage: FakeStorage,
    *,
    source: str,
    batch_id: str,
    row_count: int,
    logical_date: date = LOGICAL_DATE,
) -> None:
    manifest = BatchManifest(
        batch_id=batch_id,
        source=source,
        source_kind="postgres" if source.startswith("postgres-") else "api",
        status="completed",
        object_key="unused",
        checksum=hashlib.sha256(b"unused").hexdigest(),
        size_bytes=1,
        ingested_at=fixed_clock(),
        logical_date=logical_date,
        row_count=row_count,
        rejected_row_count=0,
        schema_version="1.0",
    )
    storage.put_object(BUCKET_ARCHIVE, manifest_key(source, batch_id), manifest.to_json().encode())


def seed_orders_batch(
    storage: FakeStorage,
    *,
    rows: int,
    manifest_rows: int | None = None,
    logical_date: date = LOGICAL_DATE,
) -> None:
    body = orders_parquet(orders_rows(rows))
    storage.put_object(BUCKET_ARCHIVE, postgres_snapshot_key("orders", logical_date), body)
    write_manifest(
        storage,
        source="postgres-orders",
        batch_id=f"postgres-orders-{logical_date:%Y%m%d}",
        row_count=rows if manifest_rows is None else manifest_rows,
        logical_date=logical_date,
    )


def seed_fx_batch(storage: FakeStorage, logical_date: date = LOGICAL_DATE) -> None:
    pages = [
        {"rates": [{"currency": "USD", "rate": 1.09}, {"currency": "GBP", "rate": 0.85}]},
        {"rates": [{"currency": "JPY", "rate": 163.2}]},
    ]
    for number, page in enumerate(pages, start=1):
        storage.put_object(
            BUCKET_ARCHIVE,
            api_page_key("fx-rates", logical_date, number),
            json.dumps(page).encode(),
        )
    write_manifest(
        storage,
        source="fx-rates",
        batch_id=f"fx-rates-{logical_date:%Y%m%d}",
        row_count=3,
        logical_date=logical_date,
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


class FlakyTrinoExecutor:
    """Fake that raises a transient catalog-auth error N times before working."""

    def __init__(self, failures: int) -> None:
        self.statements: list[str] = []
        self.failures = failures

    def execute(self, sql: str) -> None:
        self.statements.append(sql)
        if self.failures > 0 and sql.startswith("delete from"):
            self.failures -= 1
            raise TrinoQueryError(
                {
                    "type": "INTERNAL_ERROR",
                    "name": "GENERIC_INTERNAL_ERROR",
                    "message": "Not authorized: ",
                },
                "test-query",
            )

    def fetch(self, sql: str) -> list[tuple[object, ...]]:
        self.statements.append(sql)
        return []

    def statements_matching(self, prefix: str) -> list[str]:
        return [sql for sql in self.statements if sql.startswith(prefix)]


def test_load_with_retry_recovers_from_transient_not_authorized() -> None:
    storage = FakeStorage()
    seed_orders_batch(storage, rows=2)
    executor = FlakyTrinoExecutor(failures=1)
    sleeps: list[float] = []

    result = load_with_retry(
        storage,
        executor,
        TABLES["orders"],
        logical_date=LOGICAL_DATE,
        clock=fixed_clock,
        sleep=sleeps.append,
    )

    assert result.status == "loaded"
    assert result.row_count == 2
    # the failing attempt plus the successful retry each ran a DELETE first
    assert len(executor.statements_matching("delete from")) == 2
    assert sleeps  # bounded backoff was engaged


def test_load_with_retry_reraises_non_transient_immediately() -> None:
    storage = FakeStorage()
    seed_orders_batch(storage, rows=2, manifest_rows=99)  # row-count mismatch
    executor = FlakyTrinoExecutor(failures=0)
    sleeps: list[float] = []

    with pytest.raises(LoadError, match="row count mismatch"):
        load_with_retry(
            storage,
            executor,
            TABLES["orders"],
            logical_date=LOGICAL_DATE,
            clock=fixed_clock,
            sleep=sleeps.append,
        )
    assert sleeps == []


def test_load_with_retry_gives_up_after_bounded_attempts() -> None:
    storage = FakeStorage()
    seed_orders_batch(storage, rows=2)
    executor = FlakyTrinoExecutor(failures=99)
    sleeps: list[float] = []

    with pytest.raises(TrinoQueryError, match="Not authorized"):
        load_with_retry(
            storage,
            executor,
            TABLES["orders"],
            logical_date=LOGICAL_DATE,
            clock=fixed_clock,
            sleep=sleeps.append,
        )
    assert len(sleeps) == 2  # attempts=3 -> two backoff sleeps before giving up


def watermarked_executor(watermark: date | None) -> FakeTrinoExecutor:
    """Executor answering watermark queries as if the table held ``watermark``."""
    executor = FakeTrinoExecutor()
    if watermark is not None:
        executor.fetch_by_prefix["select 1 from"] = [(1,)]
        executor.fetch_by_prefix["select max("] = [(watermark,)]
    return executor


def test_table_exists_and_max_batch_date_sql_shapes() -> None:
    assert table_exists_sql(TABLES["orders"], "iceberg") == (
        "select 1 from iceberg.information_schema.tables "
        "where table_schema = 'bronze' and table_name = 'orders'"
    )
    assert max_batch_date_sql(TABLES["orders"], "iceberg") == (
        'select max("_batch_date") from iceberg.bronze.orders'
    )


def test_read_watermark_is_none_when_table_missing() -> None:
    executor = FakeTrinoExecutor()

    assert read_watermark(executor, TABLES["orders"], "iceberg") is None
    # the missing table short-circuits before the max() query is even issued
    assert executor.statements_matching("select max(") == []


def test_read_watermark_returns_max_batch_date() -> None:
    executor = FakeTrinoExecutor()
    executor.fetch_by_prefix["select 1 from"] = [(1,)]
    executor.fetch_by_prefix["select max("] = [(LOGICAL_DATE,)]

    assert read_watermark(executor, TABLES["orders"], "iceberg") == LOGICAL_DATE


def test_read_watermark_rejects_non_date_value() -> None:
    executor = FakeTrinoExecutor()
    executor.fetch_by_prefix["select 1 from"] = [(1,)]
    executor.fetch_by_prefix["select max("] = [(20260918,)]

    with pytest.raises(LoadError, match="watermark is not a date"):
        read_watermark(executor, TABLES["orders"], "iceberg")


def test_discover_archive_dates_returns_sorted_unique_dates() -> None:
    storage = FakeStorage()
    seed_orders_batch(storage, rows=1, logical_date=date(2026, 9, 18))
    seed_orders_batch(storage, rows=1, logical_date=date(2026, 9, 16))
    seed_fx_batch(storage, logical_date=date(2026, 9, 17))
    storage.put_object(BUCKET_ARCHIVE, "postgres/orders/2026/09/17/notes.txt", b"foreign")

    assert discover_archive_dates(storage, TABLES["orders"]) == (
        date(2026, 9, 16),
        date(2026, 9, 18),
    )
    assert discover_archive_dates(storage, TABLES["fx_rates"]) == (date(2026, 9, 17),)


def test_load_new_missing_table_loads_all_dates_ascending() -> None:
    storage = FakeStorage()
    seed_orders_batch(storage, rows=1, logical_date=date(2026, 9, 17))
    seed_orders_batch(storage, rows=2, logical_date=date(2026, 9, 16))
    executor = FakeTrinoExecutor()

    results = load_new(storage, executor, TABLES["orders"], clock=fixed_clock)

    assert [result.logical_date for result in results] == [date(2026, 9, 16), date(2026, 9, 17)]
    inserts = executor.statements_matching("insert into")
    assert len(inserts) == 2
    assert "'postgres-orders-20260916'" in inserts[0]
    assert "'postgres-orders-20260917'" in inserts[1]


def test_load_new_loads_only_dates_past_watermark() -> None:
    storage = FakeStorage()
    seed_orders_batch(storage, rows=1, logical_date=date(2026, 9, 16))
    seed_orders_batch(storage, rows=2, logical_date=date(2026, 9, 18))
    executor = watermarked_executor(date(2026, 9, 17))  # gap day has no raw objects

    results = load_new(storage, executor, TABLES["orders"], clock=fixed_clock)

    assert [result.logical_date for result in results] == [date(2026, 9, 18)]
    assert executor.statements_matching("insert into")[0].count("), (") == 1  # two rows


def test_load_new_up_to_date_is_noop() -> None:
    storage = FakeStorage()
    seed_orders_batch(storage, rows=1, logical_date=date(2026, 9, 16))
    executor = watermarked_executor(date(2026, 9, 16))

    results = load_new(storage, executor, TABLES["orders"], clock=fixed_clock)

    assert results == []
    assert executor.statements_matching("delete from") == []
    assert executor.statements_matching("insert into") == []


def test_load_new_fail_fast_aborts_remaining_dates() -> None:
    storage = FakeStorage()
    seed_orders_batch(
        storage, rows=1, manifest_rows=9, logical_date=date(2026, 9, 17)
    )  # row-count mismatch
    seed_orders_batch(storage, rows=1, logical_date=date(2026, 9, 18))
    executor = watermarked_executor(date(2026, 9, 16))

    with pytest.raises(LoadError, match="row count mismatch"):
        load_new(storage, executor, TABLES["orders"], clock=fixed_clock)
    # the mismatch aborted the run before the later valid date was touched
    assert executor.statements_matching("insert into") == []
