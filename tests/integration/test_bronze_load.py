"""Live raw -> disposable Bronze -> disposable dbt staging integration tests."""

# pyright: reportMissingImports=false, reportMissingTypeStubs=false

import hashlib
import json
import os
import subprocess
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Protocol

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from trino.exceptions import TrinoQueryError

from integration.lakehouse_seed import trino_scalar
from integration.namespace_ownership import ObjectMutationJournal, manifest_scoped_storage
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


class Namespace(Protocol):
    bronze: str
    silver: str

    def dbt_env(self) -> dict[str, str]: ...


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


def seed_orders(
    storage: ObjectStorage, rows: int, logical_date: date = LOGICAL_DATE
) -> BatchManifest:
    data = orders_rows(rows)
    arrays = [
        pa.array([row[index] for row in data], type=column.type)
        for index, column in enumerate(ORDERS_SNAPSHOT.columns)
    ]
    table = pa.Table.from_arrays(arrays, schema=ORDERS_SNAPSHOT.arrow_schema())
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    body = sink.getvalue().to_pybytes()
    object_key = postgres_snapshot_key("orders", logical_date)
    storage.put_object(BUCKET_ARCHIVE, object_key, body)
    manifest = BatchManifest(
        batch_id=f"postgres-orders-{logical_date:%Y%m%d}",
        source="postgres-orders",
        source_kind="postgres",
        status="completed",
        object_key=object_key,
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
    return manifest


def seed_fx(storage: ObjectMutationJournal) -> BatchManifest:
    prefix = api_batch_prefix("fx-rates", LOGICAL_DATE)
    storage.lease_prefix(BUCKET_ARCHIVE, prefix)
    for key in storage.list_object_keys(BUCKET_ARCHIVE, prefix):
        storage.delete_object(BUCKET_ARCHIVE, key)
    batch_id = f"fx-rates-{LOGICAL_DATE:%Y%m%d}"
    manifest_ref = (BUCKET_ARCHIVE, manifest_key("fx-rates", batch_id))
    storage.lease_key(*manifest_ref)
    storage.delete_object(*manifest_ref)

    pages = [
        {"rates": [{"currency": "USD", "rate": 1.091234}, {"currency": "GBP", "rate": 0.85}]},
        {"rates": [{"currency": "JPY", "rate": 163.2}]},
    ]
    for number, page in enumerate(pages, start=1):
        storage.put_object(
            BUCKET_ARCHIVE,
            api_page_key("fx-rates", LOGICAL_DATE, number),
            json.dumps(page).encode(),
        )
    manifest = BatchManifest(
        batch_id=batch_id,
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
    storage.put_object(
        BUCKET_ARCHIVE,
        manifest_key("fx-rates", manifest.batch_id),
        manifest.to_json().encode(),
    )
    return manifest


def test_postgres_bronze_load_is_idempotent(
    object_journal: ObjectMutationJournal,
    lakehouse_namespace: Namespace,
) -> None:
    manifest = seed_orders(object_journal, rows=3)
    storage = manifest_scoped_storage(object_journal, (manifest,))

    with DbapiTrinoExecutor(TrinoConfig.from_env()) as executor:
        first = load(
            storage,
            executor,
            spec_by_source("orders"),
            logical_date=LOGICAL_DATE,
            schema=lakehouse_namespace.bronze,
        )
        second = load(
            storage,
            executor,
            spec_by_source("orders"),
            logical_date=LOGICAL_DATE,
            schema=lakehouse_namespace.bronze,
        )

    assert first.row_count == second.row_count == 3
    table = f"iceberg.{lakehouse_namespace.bronze}.orders"
    where = f"where \"_batch_date\" = DATE '{LOGICAL_DATE:%Y-%m-%d}'"
    assert trino_scalar(f"select count(*) from {table} {where}") == 3
    assert trino_scalar(f'select distinct "_batch_id" from {table} {where}') == (
        f"postgres-orders-{LOGICAL_DATE:%Y%m%d}"
    )


def test_api_bronze_load_flattens_pages(
    object_journal: ObjectMutationJournal,
    lakehouse_namespace: Namespace,
) -> None:
    manifest = seed_fx(object_journal)
    storage = manifest_scoped_storage(object_journal, (manifest,))

    with DbapiTrinoExecutor(TrinoConfig.from_env()) as executor:
        result = load(
            storage,
            executor,
            spec_by_source("fx-rates"),
            logical_date=LOGICAL_DATE,
            schema=lakehouse_namespace.bronze,
        )

    assert result.row_count == 3
    table = f"iceberg.{lakehouse_namespace.bronze}.fx_rates"
    where = f"where \"_batch_date\" = DATE '{LOGICAL_DATE:%Y-%m-%d}'"
    assert trino_scalar(f'select count(distinct "_source_object") from {table} {where}') == 2


def test_dbt_staging_view_builds_from_bronze(
    object_journal: ObjectMutationJournal,
    lakehouse_namespace: Namespace,
) -> None:
    manifest = seed_orders(object_journal, rows=3)
    storage = manifest_scoped_storage(object_journal, (manifest,))
    with DbapiTrinoExecutor(TrinoConfig.from_env()) as executor:
        load(
            storage,
            executor,
            spec_by_source("orders"),
            logical_date=LOGICAL_DATE,
            schema=lakehouse_namespace.bronze,
        )

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
        env={
            **os.environ,
            "TRINO_HOST": os.environ.get("TRINO_HOST", "127.0.0.1"),
            **lakehouse_namespace.dbt_env(),
        },
    )

    assert (
        trino_scalar(f"select count(*) from iceberg.{lakehouse_namespace.silver}.stg_orders") == 3
    )


class PostCommitTransientFailure:
    """Inject one ambiguous transient error after a committed later INSERT."""

    def __init__(self, delegate: DbapiTrinoExecutor, fail_after_insert: int) -> None:
        self.delegate = delegate
        self.fail_after_insert = fail_after_insert
        self.insert_count = 0
        self.failed = False

    def execute(self, sql: str) -> None:
        self.delegate.execute(sql)  # nosec B608 -- generated by the tested Bronze loader
        if sql.startswith("insert into"):
            self.insert_count += 1
            if self.insert_count == self.fail_after_insert and not self.failed:
                self.failed = True
                raise TrinoQueryError(
                    {
                        "type": "INTERNAL_ERROR",
                        "name": "GENERIC_INTERNAL_ERROR",
                        "message": "Not authorized: injected post-commit failure",
                    },
                    "integration-injected-query",
                )

    def fetch(self, sql: str) -> list[tuple[object, ...]]:
        return self.delegate.fetch(sql)


def test_bronze_load_new_resumes_ambiguous_later_chunk(
    object_journal: ObjectMutationJournal,
    lakehouse_namespace: Namespace,
) -> None:
    """A restarted process retains completed chunks and produces unique coordinates."""
    manifest = seed_orders(object_journal, rows=10_001)
    storage = manifest_scoped_storage(object_journal, (manifest,))

    with DbapiTrinoExecutor(TrinoConfig.from_env()) as delegate:
        failing = PostCommitTransientFailure(delegate, fail_after_insert=2)
        with pytest.raises(TrinoQueryError, match="injected post-commit"):
            load_new(
                storage,
                failing,
                spec_by_source("orders"),
                schema=lakehouse_namespace.bronze,
                attempts=1,
            )

    with DbapiTrinoExecutor(TrinoConfig.from_env()) as executor:
        assert (
            load_new(
                storage,
                executor,
                spec_by_source("orders"),
                schema=lakehouse_namespace.bronze,
            )
            == []
        )

    table = f"iceberg.{lakehouse_namespace.bronze}.orders"
    counts = trino_scalar(
        f"select count(*), count(distinct row(_source_object, _source_object_row_position)) "
        f"from {table} where _batch_date = DATE '{LOGICAL_DATE:%Y-%m-%d}'"
    )
    assert counts == 10_001
    assert (
        trino_scalar(
            f"select count(distinct row(_source_object, _source_object_row_position)) "
            f"from {table} where _batch_date = DATE '{LOGICAL_DATE:%Y-%m-%d}'"
        )
        == 10_001
    )


def test_bronze_load_new_loads_only_manifest_owned_dates(
    object_journal: ObjectMutationJournal,
    lakehouse_namespace: Namespace,
) -> None:
    manifests = (
        seed_orders(object_journal, rows=3, logical_date=LOGICAL_DATE),
        seed_orders(object_journal, rows=4, logical_date=NEXT_LOGICAL_DATE),
    )
    storage = manifest_scoped_storage(object_journal, manifests)

    with DbapiTrinoExecutor(TrinoConfig.from_env()) as executor:
        first_day = load(
            storage,
            executor,
            spec_by_source("orders"),
            logical_date=LOGICAL_DATE,
            schema=lakehouse_namespace.bronze,
        )
        assert first_day.row_count == 3
        results = load_new(
            storage,
            executor,
            spec_by_source("orders"),
            schema=lakehouse_namespace.bronze,
        )
        assert (
            load_new(
                storage,
                executor,
                spec_by_source("orders"),
                schema=lakehouse_namespace.bronze,
            )
            == []
        )

    assert [result.logical_date for result in results] == [NEXT_LOGICAL_DATE]
    table = f"iceberg.{lakehouse_namespace.bronze}.orders"
    counts = {
        day: trino_scalar(
            f"select count(*) from {table} where \"_batch_date\" = DATE '{day:%Y-%m-%d}'"
        )
        for day in (LOGICAL_DATE, NEXT_LOGICAL_DATE)
    }
    assert counts == {LOGICAL_DATE: 3, NEXT_LOGICAL_DATE: 4}
