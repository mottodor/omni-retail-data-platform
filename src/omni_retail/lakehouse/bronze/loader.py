"""Idempotent Bronze loader: raw archive objects -> Iceberg via Trino SQL.

Load semantics (Phase 5 design spec §4, §6):
- raw objects are read and verified (schema, manifest row count) BEFORE any
  DML, so a failed verification never touches the existing partition;
- ``load(source, date)`` replaces the day's partition: ``DELETE`` by
  ``_batch_date`` followed by batched ``INSERT`` statements — re-running the
  same logical date (retry, backfill) cannot create duplicate rows;
- an empty day (no raw objects) is a warning-level no-op.
"""

import logging
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Literal, Protocol

import trino

from omni_retail.ingestion.common.logging import context_logger
from omni_retail.ingestion.common.manifest import BatchManifest
from omni_retail.ingestion.common.paths import BUCKET_ARCHIVE, manifest_key
from omni_retail.ingestion.common.storage import ObjectNotFoundError, ObjectStorage
from omni_retail.lakehouse.bronze.literals import date_literal, insert_statements
from omni_retail.lakehouse.bronze.readers import list_data_objects, read_rows
from omni_retail.lakehouse.bronze.specs import (
    BATCH_DATE,
    BATCH_ID,
    INGESTED_AT,
    SCHEMA_BRONZE,
    SOURCE_OBJECT,
    BronzeTableSpec,
)

Clock = Callable[[], datetime]

logger = logging.getLogger(__name__)


class LoadError(Exception):
    """Explicit Bronze load failure (manifest missing, row-count mismatch)."""


@dataclass(frozen=True)
class TrinoConfig:
    """Connection settings for the local (unauthenticated) Trino coordinator."""

    host: str = "127.0.0.1"
    port: int = 8080
    catalog: str = "iceberg"
    user: str = "omni"

    @classmethod
    def from_env(cls) -> "TrinoConfig":
        return cls(
            host=os.environ.get("TRINO_HOST", cls.host),
            port=int(os.environ.get("TRINO_PORT", str(cls.port))),
            catalog=os.environ.get("TRINO_CATALOG", cls.catalog),
            user=os.environ.get("TRINO_USER", cls.user),
        )


class TrinoExecutor(Protocol):
    """SQL execution boundary (trino.dbapi connection or a test fake)."""

    def execute(self, sql: str) -> None: ...


class DbapiTrinoExecutor:
    """Lazily-connecting autocommit executor over one trino.dbapi connection."""

    def __init__(self, config: TrinoConfig) -> None:
        self._config = config
        self._connection: trino.dbapi.Connection | None = None

    def _connect(self) -> trino.dbapi.Connection:
        if self._connection is None:
            self._connection = trino.dbapi.connect(  # type: ignore[no-untyped-call]
                host=self._config.host,
                port=self._config.port,
                user=self._config.user,
                catalog=self._config.catalog,
            )
        return self._connection

    def execute(self, sql: str) -> None:
        cursor = self._connect().cursor()
        cursor.execute(sql)

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()  # type: ignore[no-untyped-call]
            self._connection = None

    def __enter__(self) -> "DbapiTrinoExecutor":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


@dataclass(frozen=True)
class LoadResult:
    """Outcome of one (source, logical date) Bronze load."""

    source: str
    batch_id: str
    logical_date: date
    row_count: int
    status: Literal["loaded", "empty"]


def create_schema_sql(catalog: str) -> str:
    return f"create schema if not exists {catalog}.{SCHEMA_BRONZE}"


def create_table_sql(spec: BronzeTableSpec, catalog: str) -> str:
    columns = ", ".join(f'"{column.name}" {column.trino_type}' for column in spec.all_columns)
    return (
        f"create table if not exists {catalog}.{SCHEMA_BRONZE}.{spec.name} "
        f"({columns}) with (partitioning = ARRAY['_batch_date'])"
    )


def delete_partition_sql(spec: BronzeTableSpec, catalog: str, logical_date: date) -> str:
    return (
        f"delete from {catalog}.{SCHEMA_BRONZE}.{spec.name} "
        f'where "{BATCH_DATE}" = {date_literal(logical_date)}'
    )


def load(
    storage: ObjectStorage,
    executor: TrinoExecutor,
    spec: BronzeTableSpec,
    *,
    logical_date: date,
    clock: Clock | None = None,
    catalog: str = "iceberg",
) -> LoadResult:
    """Load one (source, logical date) batch into Bronze; idempotent per day."""
    effective_clock: Clock = clock or (lambda: datetime.now(UTC))
    log = context_logger(__name__, source=spec.source_name, logical_date=logical_date.isoformat())
    batch_id = spec.batch_id(logical_date)

    objects = list_data_objects(storage, spec, logical_date)
    if not objects:
        log.warning("bronze batch empty: no raw objects under %s", spec.object_prefix(logical_date))
        return LoadResult(spec.source_key, batch_id, logical_date, 0, "empty")

    manifest = _read_manifest(storage, spec, logical_date)
    ingested_at = effective_clock()
    rows: list[dict[str, object]] = []
    for object_key in objects:
        body = storage.get_object(BUCKET_ARCHIVE, object_key)
        for row in read_rows(spec, object_key, body):
            rows.append(
                {
                    **row,
                    BATCH_ID: batch_id,
                    BATCH_DATE: logical_date,
                    SOURCE_OBJECT: object_key,
                    INGESTED_AT: ingested_at,
                }
            )

    if len(rows) != manifest.row_count:
        raise LoadError(
            f"{spec.source_key}: row count mismatch for {logical_date}: "
            f"raw rows={len(rows)} manifest rows={manifest.row_count} "
            "(partition not modified)"
        )

    executor.execute(create_schema_sql(catalog))
    executor.execute(create_table_sql(spec, catalog))
    executor.execute(delete_partition_sql(spec, catalog, logical_date))
    statements = insert_statements(spec, rows, catalog=catalog)
    for statement in statements:
        executor.execute(statement)
    log.info(
        "bronze load completed: batch_id=%s objects=%d row_count=%d statements=%d",
        batch_id,
        len(objects),
        len(rows),
        len(statements),
    )
    return LoadResult(spec.source_key, batch_id, logical_date, len(rows), "loaded")


def _read_manifest(
    storage: ObjectStorage, spec: BronzeTableSpec, logical_date: date
) -> BatchManifest:
    key = manifest_key(spec.source_name, spec.batch_id(logical_date))
    try:
        body = storage.get_object(BUCKET_ARCHIVE, key)
    except ObjectNotFoundError as error:
        raise LoadError(f"manifest not found: s3://{BUCKET_ARCHIVE}/{key}") from error
    return BatchManifest.from_json(body.decode())
