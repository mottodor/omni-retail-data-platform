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
import re
import time
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

_SCHEMA_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,127}$")


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

    def fetch(self, sql: str) -> list[tuple[object, ...]]: ...


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

    def fetch(self, sql: str) -> list[tuple[object, ...]]:
        cursor = self._connect().cursor()
        cursor.execute(sql)
        return list(cursor.fetchall())

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


def validate_schema_name(schema: str) -> str:
    """Validate an unquoted Trino schema identifier before SQL interpolation."""
    if not _SCHEMA_NAME_PATTERN.fullmatch(schema):
        raise ValueError(
            f"invalid schema name {schema!r}; expected lower-case letters, digits, "
            "underscores, starting with a letter"
        )
    return schema


def bronze_schema_from_env() -> str:
    """Resolve the configurable Bronze schema with the production default."""
    return validate_schema_name(os.environ.get("ICEBERG_BRONZE_SCHEMA", SCHEMA_BRONZE))


def create_schema_sql(catalog: str, schema: str = SCHEMA_BRONZE) -> str:
    return f"create schema if not exists {catalog}.{validate_schema_name(schema)}"


def create_table_sql(spec: BronzeTableSpec, catalog: str, schema: str = SCHEMA_BRONZE) -> str:
    schema = validate_schema_name(schema)
    columns = ", ".join(f'"{column.name}" {column.trino_type}' for column in spec.all_columns)
    return (
        f"create table if not exists {catalog}.{schema}.{spec.name} "
        f"({columns}) with (partitioning = ARRAY['_batch_date'])"
    )


def delete_partition_sql(
    spec: BronzeTableSpec,
    catalog: str,
    logical_date: date,
    schema: str = SCHEMA_BRONZE,
) -> str:
    schema = validate_schema_name(schema)
    return (
        f"delete from {catalog}.{schema}.{spec.name} "
        f'where "{BATCH_DATE}" = {date_literal(logical_date)}'
    )


def table_exists_sql(spec: BronzeTableSpec, catalog: str, schema: str = SCHEMA_BRONZE) -> str:
    schema = validate_schema_name(schema)
    return (
        f"select 1 from {catalog}.information_schema.tables "
        f"where table_schema = '{schema}' and table_name = '{spec.name}'"
    )


def max_batch_date_sql(spec: BronzeTableSpec, catalog: str, schema: str = SCHEMA_BRONZE) -> str:
    schema = validate_schema_name(schema)
    return f'select max("{BATCH_DATE}") from {catalog}.{schema}.{spec.name}'


def read_watermark(
    executor: TrinoExecutor,
    spec: BronzeTableSpec,
    catalog: str,
    schema: str = SCHEMA_BRONZE,
) -> date | None:
    """Highest logical date already present in the source's Bronze table.

    The Bronze table itself is the watermark state: a missing table or an
    empty one means "nothing loaded yet". The check goes through
    ``information_schema`` instead of catching a missing-table query error.
    """
    if not executor.fetch(table_exists_sql(spec, catalog, schema)):
        return None
    rows = executor.fetch(max_batch_date_sql(spec, catalog, schema))
    if not rows or rows[0][0] is None:
        return None
    value = rows[0][0]
    if not isinstance(value, date):
        raise LoadError(
            f"{spec.source_key}: watermark is not a date: {value!r} "
            f"(from {max_batch_date_sql(spec, catalog, schema)})"
        )
    return value


def discover_archive_dates(storage: ObjectStorage, spec: BronzeTableSpec) -> tuple[date, ...]:
    """Sorted logical dates that have at least one raw data object archived."""
    suffix = "." + spec.data_object_suffix
    dates = {
        parsed
        for key in storage.list_object_keys(BUCKET_ARCHIVE, spec.root_prefix)
        if key.endswith(suffix)
        for parsed in (spec.logical_date_from_key(key),)
        if parsed is not None
    }
    return tuple(sorted(dates))


def load(
    storage: ObjectStorage,
    executor: TrinoExecutor,
    spec: BronzeTableSpec,
    *,
    logical_date: date,
    clock: Clock | None = None,
    catalog: str = "iceberg",
    schema: str = SCHEMA_BRONZE,
) -> LoadResult:
    """Load one (source, logical date) batch into Bronze; idempotent per day."""
    schema = validate_schema_name(schema)
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

    executor.execute(create_schema_sql(catalog, schema))
    executor.execute(create_table_sql(spec, catalog, schema))
    executor.execute(delete_partition_sql(spec, catalog, logical_date, schema))
    statements = insert_statements(spec, rows, catalog=catalog, schema=schema)
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


#: Trino/Polaris catalog errors that are safe to retry by re-running the
#: whole idempotent per-day load (DELETE partition + INSERT). Known trigger:
#: trinodb/trino#30816 (fixed in the unreleased 484) — per-operation OAuth2
#: token fetches occasionally send catalog requests unauthenticated, and
#: Polaris answers empty-body 401 "Not authorized" responses.
_TRANSIENT_CATALOG_MARKERS: tuple[str, ...] = ("Not authorized",)


def is_transient_catalog_error(error: BaseException) -> bool:
    """Classify trino errors that a retry of the same load can survive."""
    if not isinstance(error, trino.exceptions.Error):
        return False
    message = getattr(error, "message", "")
    return isinstance(message, str) and any(
        marker in message for marker in _TRANSIENT_CATALOG_MARKERS
    )


def load_with_retry(
    storage: ObjectStorage,
    executor: TrinoExecutor,
    spec: BronzeTableSpec,
    *,
    logical_date: date,
    clock: Clock | None = None,
    catalog: str = "iceberg",
    schema: str = SCHEMA_BRONZE,
    attempts: int = 3,
    sleep: Callable[[float], None] = time.sleep,
) -> LoadResult:
    """Run :func:`load` with bounded retries on transient catalog-auth errors.

    Non-transient failures (row-count mismatch, schema drift) propagate
    immediately; each retry re-executes the whole idempotent day load.
    """
    log = context_logger(__name__, source=spec.source_name, logical_date=logical_date.isoformat())
    for attempt in range(1, attempts + 1):
        try:
            return load(
                storage,
                executor,
                spec,
                logical_date=logical_date,
                clock=clock,
                catalog=catalog,
                schema=schema,
            )
        except trino.exceptions.Error as error:
            if attempt == attempts or not is_transient_catalog_error(error):
                raise
            backoff_seconds = float(2**attempt)
            log.warning(
                "transient catalog error, retrying bronze load: attempt=%d/%d "
                "backoff_seconds=%.0f error_type=%s error_message=%s",
                attempt,
                attempts,
                backoff_seconds,
                type(error).__name__,
                getattr(error, "message", ""),
            )
            sleep(backoff_seconds)
    raise AssertionError("unreachable: load_with_retry exhausted attempts without raising")


def load_new(
    storage: ObjectStorage,
    executor: TrinoExecutor,
    spec: BronzeTableSpec,
    *,
    clock: Clock | None = None,
    catalog: str = "iceberg",
    schema: str = SCHEMA_BRONZE,
    attempts: int = 3,
    sleep: Callable[[float], None] = time.sleep,
) -> list[LoadResult]:
    """Load every archived logical date newer than the Bronze watermark.

    Watermark-driven counterpart of repeated ``run`` calls: unlike a
    dataset-triggered logical date, ``max(_batch_date)`` always addresses the
    producer's next unloaded day, and days with no raw objects are simply not
    discovered. Dates load in ascending order; the first failure aborts the
    source (fail fast) and a restart resumes at the watermark.
    """
    log = context_logger(__name__, source=spec.source_name, command="run-new")
    schema = validate_schema_name(schema)
    watermark = read_watermark(executor, spec, catalog, schema)
    pending = [
        logical_date
        for logical_date in discover_archive_dates(storage, spec)
        if watermark is None or logical_date > watermark
    ]
    results: list[LoadResult] = []
    for logical_date in pending:
        result = load_with_retry(
            storage,
            executor,
            spec,
            logical_date=logical_date,
            clock=clock,
            catalog=catalog,
            schema=schema,
            attempts=attempts,
            sleep=sleep,
        )
        results.append(result)
        log.info(
            "run-new progress: source=%s logical_date=%s status=%s row_count=%d",
            result.source,
            result.logical_date.isoformat(),
            result.status,
            result.row_count,
        )
    if not results:
        log.info(
            "run-new up-to-date: source=%s watermark=%s pending_dates=0",
            spec.source_key,
            watermark.isoformat() if watermark is not None else "none",
        )
    return results
