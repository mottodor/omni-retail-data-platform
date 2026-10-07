"""Idempotent, restartable raw-archive to Iceberg Bronze loader."""

# pyright: reportMissingImports=false

import logging
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Literal

import trino

from omni_retail.ingestion.common.logging import context_logger
from omni_retail.ingestion.common.manifest import BatchManifest
from omni_retail.ingestion.common.paths import BUCKET_ARCHIVE, manifest_key
from omni_retail.ingestion.common.storage import ObjectNotFoundError, ObjectStorage
from omni_retail.lakehouse.bronze.literals import (
    InsertChunk,
    date_literal,
    insert_chunks,
    varchar_literal,
)
from omni_retail.lakehouse.bronze.readers import list_data_objects, read_rows
from omni_retail.lakehouse.bronze.specs import (
    BATCH_DATE,
    BATCH_ID,
    INGESTED_AT,
    SCHEMA_BRONZE,
    SOURCE_OBJECT,
    SOURCE_OBJECT_ROW_POSITION,
    BronzeTableSpec,
)
from omni_retail.lakehouse.trino import (
    DbapiTrinoExecutor as DbapiTrinoExecutor,
)
from omni_retail.lakehouse.trino import TrinoConfig as TrinoConfig
from omni_retail.lakehouse.trino import TrinoExecutor as TrinoExecutor
from omni_retail.lakehouse.trino import (
    is_transient_catalog_error as is_transient_catalog_error,
)
from omni_retail.lakehouse.trino import is_transient_trino_error as is_transient_trino_error
from omni_retail.lakehouse.trino import validate_schema_name as validate_schema_name

__all__ = (
    "DbapiTrinoExecutor",
    "TrinoConfig",
    "TrinoExecutor",
    "is_transient_catalog_error",
    "is_transient_trino_error",
    "validate_schema_name",
)

Clock = Callable[[], datetime]
logger = logging.getLogger(__name__)


class LoadError(Exception):
    """Non-transient Bronze failure (archive, manifest, schema, or integrity)."""


@dataclass(frozen=True)
class LoadResult:
    source: str
    batch_id: str
    logical_date: date
    row_count: int
    status: Literal["loaded", "empty"]


@dataclass(frozen=True)
class _PreparedBatch:
    manifest: BatchManifest
    rows: list[dict[str, object]]
    object_count: int


def bronze_schema_from_env() -> str:
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


def add_row_position_sql(spec: BronzeTableSpec, catalog: str, schema: str = SCHEMA_BRONZE) -> str:
    """Safe Iceberg schema evolution for Bronze tables created before issue #15."""
    schema = validate_schema_name(schema)
    return (
        f"alter table {catalog}.{schema}.{spec.name} add column if not exists "
        f'"{SOURCE_OBJECT_ROW_POSITION}" bigint'
    )


def _execute_idempotent_ddl_with_retry(
    executor: TrinoExecutor,
    sql: str,
    *,
    attempts: int,
    sleep: Callable[[float], None],
    log: logging.LoggerAdapter[logging.Logger],
) -> None:
    """Retry one replay-safe DDL statement on recognized catalog failures."""
    if attempts < 1:
        raise ValueError("DDL attempts must be at least 1")
    for attempt in range(1, attempts + 1):
        try:
            # pi-lens-ignore: python-sql-injection
            executor.execute(sql)
            return
        except trino.exceptions.Error as error:
            if not is_transient_catalog_error(error):
                raise
            if attempt == attempts:
                log.error(
                    "transient catalog error; idempotent DDL retry exhausted: "
                    "attempts=%d error_type=%s error_message=%s",
                    attempts,
                    type(error).__name__,
                    getattr(error, "message", ""),
                )
                raise
            backoff_seconds = float(2**attempt)
            log.warning(
                "transient catalog error; retrying idempotent DDL: "
                "attempt=%d/%d backoff_seconds=%.0f error_type=%s error_message=%s",
                attempt,
                attempts,
                backoff_seconds,
                type(error).__name__,
                getattr(error, "message", ""),
            )
            sleep(backoff_seconds)
    raise AssertionError("unreachable")


def ensure_table(
    executor: TrinoExecutor,
    spec: BronzeTableSpec,
    catalog: str,
    schema: str,
    *,
    attempts: int = 3,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Create or evolve one Bronze table with bounded idempotent-DDL retries."""
    log = context_logger(__name__, source=spec.source_name, schema=schema, table=spec.name)
    statements = (
        create_schema_sql(catalog, schema),
        create_table_sql(spec, catalog, schema),
        add_row_position_sql(spec, catalog, schema),
    )
    for sql in statements:
        # pi-lens-ignore: python-sql-injection
        _execute_idempotent_ddl_with_retry(
            executor,
            sql,
            attempts=attempts,
            sleep=sleep,
            log=log,
        )


def delete_partition_sql(
    spec: BronzeTableSpec, catalog: str, logical_date: date, schema: str = SCHEMA_BRONZE
) -> str:
    schema = validate_schema_name(schema)
    return (
        f"delete from {catalog}.{schema}.{spec.name} "
        f'where "{BATCH_DATE}" = {date_literal(logical_date)}'
    )


def delete_chunk_sql(
    spec: BronzeTableSpec,
    chunk: InsertChunk,
    catalog: str,
    logical_date: date,
    schema: str = SCHEMA_BRONZE,
) -> str:
    """Delete only coordinates represented by one INSERT chunk."""
    schema = validate_schema_name(schema)
    predicates = " or ".join(
        f'("{SOURCE_OBJECT}" = {varchar_literal(item.source_object)} and '
        f'"{SOURCE_OBJECT_ROW_POSITION}" between {item.first_row_position} '
        f"and {item.last_row_position})"
        for item in chunk.ranges
    )
    return (
        f"delete from {catalog}.{schema}.{spec.name} "
        f'where "{BATCH_DATE}" = {date_literal(logical_date)} and ({predicates})'
    )


def table_exists_sql(spec: BronzeTableSpec, catalog: str, schema: str = SCHEMA_BRONZE) -> str:
    schema = validate_schema_name(schema)
    return (
        f"select 1 from {catalog}.information_schema.tables "
        f"where table_schema = '{schema}' and table_name = '{spec.name}'"
    )


def max_batch_date_sql(spec: BronzeTableSpec, catalog: str, schema: str = SCHEMA_BRONZE) -> str:
    return f'select max("{BATCH_DATE}") from {catalog}.{validate_schema_name(schema)}.{spec.name}'


def read_watermark(
    executor: TrinoExecutor, spec: BronzeTableSpec, catalog: str, schema: str = SCHEMA_BRONZE
) -> date | None:
    if not executor.fetch(table_exists_sql(spec, catalog, schema)):
        return None
    rows = executor.fetch(max_batch_date_sql(spec, catalog, schema))
    if not rows or rows[0][0] is None:
        return None
    value = rows[0][0]
    if not isinstance(value, date):
        raise LoadError(f"{spec.source_key}: watermark is not a date: {value!r}")
    return value


def discover_archive_dates(storage: ObjectStorage, spec: BronzeTableSpec) -> tuple[date, ...]:
    suffix = "." + spec.data_object_suffix
    return tuple(
        sorted(
            {
                parsed
                for key in storage.list_object_keys(BUCKET_ARCHIVE, spec.root_prefix)
                if key.endswith(suffix)
                for parsed in (spec.logical_date_from_key(key),)
                if parsed is not None
            }
        )
    )


def _read_manifest(
    storage: ObjectStorage, spec: BronzeTableSpec, logical_date: date
) -> BatchManifest:
    key = manifest_key(spec.source_name, spec.batch_id(logical_date))
    try:
        return BatchManifest.from_json(storage.get_object(BUCKET_ARCHIVE, key).decode())
    except ObjectNotFoundError as error:
        raise LoadError(f"manifest not found: s3://{BUCKET_ARCHIVE}/{key}") from error


def _prepare_batch(
    storage: ObjectStorage, spec: BronzeTableSpec, logical_date: date, clock: Clock
) -> _PreparedBatch | None:
    objects = list_data_objects(storage, spec, logical_date)
    if not objects:
        return None
    manifest = _read_manifest(storage, spec, logical_date)
    ingested_at = clock()
    rows: list[dict[str, object]] = []
    for object_key in objects:
        for position, row in enumerate(
            read_rows(spec, object_key, storage.get_object(BUCKET_ARCHIVE, object_key))
        ):
            rows.append(
                {
                    **row,
                    BATCH_ID: spec.batch_id(logical_date),
                    BATCH_DATE: logical_date,
                    SOURCE_OBJECT: object_key,
                    SOURCE_OBJECT_ROW_POSITION: position,
                    INGESTED_AT: ingested_at,
                }
            )
    if len(rows) != manifest.row_count:
        raise LoadError(
            f"{spec.source_key}: row count mismatch for {logical_date}: raw rows={len(rows)} "
            f"manifest rows={manifest.row_count} (Bronze not modified)"
        )
    return _PreparedBatch(manifest, rows, len(objects))


def load(
    storage: ObjectStorage,
    executor: TrinoExecutor,
    spec: BronzeTableSpec,
    *,
    logical_date: date,
    clock: Clock | None = None,
    catalog: str = "iceberg",
    schema: str = SCHEMA_BRONZE,
    ddl_attempts: int = 3,
    ddl_sleep: Callable[[float], None] = time.sleep,
) -> LoadResult:
    """Explicit-date load: preserve the established full partition replacement contract."""
    schema = validate_schema_name(schema)
    prepared = _prepare_batch(storage, spec, logical_date, clock or (lambda: datetime.now(UTC)))
    if prepared is None:
        logger.warning(
            "bronze batch empty: source=%s logical_date=%s", spec.source_key, logical_date
        )
        return LoadResult(spec.source_key, spec.batch_id(logical_date), logical_date, 0, "empty")
    ensure_table(
        executor,
        spec,
        catalog,
        schema,
        attempts=ddl_attempts,
        sleep=ddl_sleep,
    )
    executor.execute(delete_partition_sql(spec, catalog, logical_date, schema))
    chunks = insert_chunks(spec, prepared.rows, catalog=catalog, schema=schema)
    for chunk in chunks:
        executor.execute(chunk.sql)
    context_logger(__name__, source=spec.source_name, logical_date=logical_date.isoformat()).info(
        "bronze partition replaced: batch_id=%s objects=%d row_count=%d chunks=%d",
        prepared.manifest.batch_id,
        prepared.object_count,
        len(prepared.rows),
        len(chunks),
    )
    return LoadResult(
        spec.source_key, spec.batch_id(logical_date), logical_date, len(prepared.rows), "loaded"
    )


def _execute_with_retry(
    executor: TrinoExecutor,
    delete_sql: str,
    insert_sql: str,
    *,
    attempts: int,
    sleep: Callable[[float], None],
    log: logging.LoggerAdapter[logging.Logger],
) -> None:
    for attempt in range(1, attempts + 1):
        try:
            # pi-lens-ignore: python-sql-injection
            executor.execute(delete_sql)
            # pi-lens-ignore: python-sql-injection
            executor.execute(insert_sql)
            return
        except trino.exceptions.Error as error:
            if attempt == attempts or not is_transient_catalog_error(error):
                raise
            backoff_seconds = float(2**attempt)
            log.warning(
                "transient catalog error; retrying chunk: attempt=%d/%d backoff_seconds=%.0f "
                "error_type=%s error_message=%s",
                attempt,
                attempts,
                backoff_seconds,
                type(error).__name__,
                getattr(error, "message", ""),
            )
            sleep(backoff_seconds)
    raise AssertionError("unreachable")


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
    """Bounded full-date retry for explicit date loads."""
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
                # The outer explicit-date retry owns this operation's budget.
                ddl_attempts=1,
                ddl_sleep=sleep,
            )
        except trino.exceptions.Error as error:
            if attempt == attempts or not is_transient_catalog_error(error):
                raise
            backoff_seconds = float(2**attempt)
            log.warning(
                "transient catalog error; retrying date: attempt=%d/%d backoff_seconds=%.0f",
                attempt,
                attempts,
                backoff_seconds,
            )
            sleep(backoff_seconds)
    raise AssertionError("unreachable")


def _chunk_count_sql(
    spec: BronzeTableSpec,
    chunk: InsertChunk,
    catalog: str,
    logical_date: date,
    schema: str,
) -> str:
    predicate = delete_chunk_sql(spec, chunk, catalog, logical_date, schema).split(" where ", 1)[1]
    return (
        f'select count(*), count(distinct row("{SOURCE_OBJECT}", '
        f'"{SOURCE_OBJECT_ROW_POSITION}")) from {catalog}.{schema}.{spec.name} where {predicate}'
    )


def _chunk_is_complete(
    executor: TrinoExecutor,
    spec: BronzeTableSpec,
    chunk: InsertChunk,
    catalog: str,
    logical_date: date,
    schema: str,
) -> bool:
    rows = executor.fetch(_chunk_count_sql(spec, chunk, catalog, logical_date, schema))
    return bool(rows and tuple(rows[0]) == (chunk.row_count, chunk.row_count))


def _verify_batch(
    executor: TrinoExecutor,
    spec: BronzeTableSpec,
    prepared: _PreparedBatch,
    catalog: str,
    logical_date: date,
    schema: str,
) -> None:
    rows = executor.fetch(
        f'select count(*), count(distinct row("{SOURCE_OBJECT}", '
        f'"{SOURCE_OBJECT_ROW_POSITION}")) from {catalog}.{schema}.{spec.name} '
        f'where "{BATCH_DATE}" = {date_literal(logical_date)}'
    )
    expected = (prepared.manifest.row_count, prepared.manifest.row_count)
    if not rows or tuple(rows[0]) != expected:
        raise LoadError(
            f"{spec.source_key}: Bronze integrity mismatch for {logical_date}: "
            f"expected count/unique={expected}, actual={rows[0] if rows else None}"
        )


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
    """Resume archived days using independently replaceable source-object chunks."""
    schema = validate_schema_name(schema)
    watermark = read_watermark(executor, spec, catalog, schema)
    # Include the watermark itself: it may be a partially committed day after a crash.
    pending = [
        day
        for day in discover_archive_dates(storage, spec)
        if watermark is None or day >= watermark
    ]
    results: list[LoadResult] = []
    effective_clock = clock or (lambda: datetime.now(UTC))
    for logical_date in pending:
        prepared = _prepare_batch(storage, spec, logical_date, effective_clock)
        if prepared is None:
            continue
        ensure_table(
            executor,
            spec,
            catalog,
            schema,
            attempts=attempts,
            sleep=sleep,
        )
        chunks = insert_chunks(spec, prepared.rows, catalog=catalog, schema=schema)
        changed = False
        for index, chunk in enumerate(chunks, start=1):
            if _chunk_is_complete(executor, spec, chunk, catalog, logical_date, schema):
                continue
            changed = True
            log = context_logger(
                __name__,
                source=spec.source_name,
                logical_date=logical_date.isoformat(),
                chunk=index,
            )
            _execute_with_retry(
                executor,
                delete_chunk_sql(spec, chunk, catalog, logical_date, schema),
                chunk.sql,
                attempts=attempts,
                sleep=sleep,
                log=log,
            )
        _verify_batch(executor, spec, prepared, catalog, logical_date, schema)
        if changed:
            results.append(
                LoadResult(
                    spec.source_key,
                    spec.batch_id(logical_date),
                    logical_date,
                    len(prepared.rows),
                    "loaded",
                )
            )
    return results
