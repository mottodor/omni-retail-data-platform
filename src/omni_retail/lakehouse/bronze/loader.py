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

from omni_retail.ingestion.common.checksum import compute_checksum
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
from omni_retail.lakehouse.bronze.readers import list_data_objects, read_file_rows, read_rows
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
    batch_ids: tuple[str, ...]
    logical_date: date
    row_count: int
    status: Literal["loaded", "empty"]

    @property
    def batch_id(self) -> str:
        """Backward-compatible scalar identity for single-manifest loads."""
        if len(self.batch_ids) != 1:
            raise ValueError(f"{self.source}: expected one batch id, found {len(self.batch_ids)}")
        return self.batch_ids[0]


@dataclass(frozen=True)
class _PreparedBatch:
    manifests: tuple[BatchManifest, ...]
    rows: list[dict[str, object]]
    object_count: int
    expected_row_count: int

    @property
    def batch_ids(self) -> tuple[str, ...]:
        return tuple(manifest.batch_id for manifest in self.manifests)


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


def _file_manifests(storage: ObjectStorage, spec: BronzeTableSpec) -> tuple[BatchManifest, ...]:
    prefix = f"_manifests/{spec.source_name}/"
    manifests: list[BatchManifest] = []
    for key in storage.list_object_keys(BUCKET_ARCHIVE, prefix):
        try:
            manifest = BatchManifest.from_json(storage.get_object(BUCKET_ARCHIVE, key).decode())
        except (KeyError, TypeError, ValueError, UnicodeDecodeError) as error:
            raise LoadError(f"invalid manifest s3://{BUCKET_ARCHIVE}/{key}: {error}") from error
        if manifest.source != spec.source_name or manifest.source_kind != "file":
            raise LoadError(
                f"{key}: manifest identity mismatch: source={manifest.source!r} "
                f"source_kind={manifest.source_kind!r}"
            )
        if manifest.status not in {"completed", "rejected", "duplicate"}:
            raise LoadError(f"{key}: unsupported file manifest status {manifest.status!r}")
        if manifest.logical_date is None:
            raise LoadError(f"{key}: file manifest has no logical_date")
        if manifest.status == "completed":
            if manifest.schema_version != spec.schema_version:
                raise LoadError(
                    f"{key}: schema version mismatch: manifest={manifest.schema_version!r} "
                    f"Bronze={spec.schema_version!r}"
                )
            if not 0 <= manifest.rejected_row_count <= manifest.row_count:
                raise LoadError(
                    f"{key}: invalid row counts: rows={manifest.row_count} "
                    f"rejected={manifest.rejected_row_count}"
                )
            expected_prefix = spec.object_prefix(manifest.logical_date)
            if not manifest.object_key.startswith(
                expected_prefix
            ) or not manifest.object_key.endswith("." + spec.data_object_suffix):
                raise LoadError(
                    f"{key}: completed manifest object is outside the expected archive path: "
                    f"{manifest.object_key!r}"
                )
            expected_batch_id = f"{spec.source_name}-{manifest.checksum[:16]}"
            if manifest.batch_id != expected_batch_id:
                raise LoadError(
                    f"{key}: batch/checksum mismatch: expected {expected_batch_id!r}, "
                    f"got {manifest.batch_id!r}"
                )
        manifests.append(manifest)
    return tuple(manifests)


def discover_archive_dates(storage: ObjectStorage, spec: BronzeTableSpec) -> tuple[date, ...]:
    suffix = "." + spec.data_object_suffix
    object_dates = {
        parsed
        for key in storage.list_object_keys(BUCKET_ARCHIVE, spec.root_prefix)
        if spec.kind == "file" or key.endswith(suffix)
        for parsed in (spec.logical_date_from_key(key),)
        if parsed is not None
    }
    if spec.kind == "file":
        object_dates.update(
            manifest.logical_date
            for manifest in _file_manifests(storage, spec)
            if manifest.status == "completed" and manifest.logical_date is not None
        )
    return tuple(sorted(object_dates))


def _read_manifest(
    storage: ObjectStorage, spec: BronzeTableSpec, logical_date: date
) -> BatchManifest:
    key = manifest_key(spec.source_name, spec.batch_id(logical_date))
    try:
        return BatchManifest.from_json(storage.get_object(BUCKET_ARCHIVE, key).decode())
    except ObjectNotFoundError as error:
        raise LoadError(f"manifest not found: s3://{BUCKET_ARCHIVE}/{key}") from error


def _prepare_file_batch(
    storage: ObjectStorage,
    spec: BronzeTableSpec,
    logical_date: date,
    clock: Clock,
) -> _PreparedBatch | None:
    manifests = tuple(
        sorted(
            (
                manifest
                for manifest in _file_manifests(storage, spec)
                if manifest.status == "completed" and manifest.logical_date == logical_date
            ),
            key=lambda manifest: manifest.object_key,
        )
    )
    objects = storage.list_object_keys(BUCKET_ARCHIVE, spec.object_prefix(logical_date))
    manifest_objects = {manifest.object_key for manifest in manifests}
    archive_objects = set(objects)
    missing = sorted(manifest_objects - archive_objects)
    orphaned = sorted(archive_objects - manifest_objects)
    if missing or orphaned:
        raise LoadError(
            f"{spec.source_key}: archive/manifest mismatch for {logical_date}: "
            f"missing={missing}, orphaned={orphaned} (Bronze not modified)"
        )
    if len(manifest_objects) != len(manifests):
        raise LoadError(
            f"{spec.source_key}: multiple completed manifests own the same object for "
            f"{logical_date}"
        )
    if not manifests:
        return None

    ingested_at = clock()
    rows: list[dict[str, object]] = []
    expected_row_count = 0
    for manifest in manifests:
        body = storage.get_object(BUCKET_ARCHIVE, manifest.object_key)
        actual_checksum = compute_checksum(body)
        if len(body) != manifest.size_bytes or actual_checksum != manifest.checksum:
            raise LoadError(
                f"{spec.source_key}: archived object integrity mismatch for "
                f"{manifest.object_key}: expected size/checksum="
                f"{manifest.size_bytes}/{manifest.checksum}, actual="
                f"{len(body)}/{actual_checksum} (Bronze not modified)"
            )
        read_result = read_file_rows(spec, manifest.object_key, body)
        if (
            read_result.row_count != manifest.row_count
            or read_result.rejected_row_count != manifest.rejected_row_count
        ):
            raise LoadError(
                f"{spec.source_key}: row count mismatch for {manifest.object_key}: "
                f"raw total/rejected={read_result.row_count}/"
                f"{read_result.rejected_row_count}, manifest total/rejected="
                f"{manifest.row_count}/{manifest.rejected_row_count} "
                "(Bronze not modified)"
            )
        accepted_count = manifest.row_count - manifest.rejected_row_count
        if len(read_result.rows) != accepted_count:
            raise LoadError(
                f"{spec.source_key}: accepted row mismatch for {manifest.object_key}: "
                f"raw={len(read_result.rows)}, manifest={accepted_count} "
                "(Bronze not modified)"
            )
        expected_row_count += accepted_count
        context_logger(
            __name__,
            source=spec.source_name,
            logical_date=logical_date.isoformat(),
            batch_id=manifest.batch_id,
            source_object=manifest.object_key,
            checksum=manifest.checksum,
        ).info(
            "file batch prepared: row_count=%d accepted_row_count=%d rejected_row_count=%d",
            manifest.row_count,
            accepted_count,
            manifest.rejected_row_count,
        )
        for positioned in read_result.rows:
            rows.append(
                {
                    **positioned.values,
                    BATCH_ID: manifest.batch_id,
                    BATCH_DATE: logical_date,
                    SOURCE_OBJECT: manifest.object_key,
                    SOURCE_OBJECT_ROW_POSITION: positioned.row_position,
                    INGESTED_AT: ingested_at,
                }
            )
    return _PreparedBatch(manifests, rows, len(objects), expected_row_count)


def _prepare_batch(
    storage: ObjectStorage, spec: BronzeTableSpec, logical_date: date, clock: Clock
) -> _PreparedBatch | None:
    if spec.kind == "file":
        return _prepare_file_batch(storage, spec, logical_date, clock)

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
    return _PreparedBatch((manifest,), rows, len(objects), manifest.row_count)


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
        empty_batch_ids = () if spec.kind == "file" else (spec.batch_id(logical_date),)
        return LoadResult(spec.source_key, empty_batch_ids, logical_date, 0, "empty")
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
        "bronze partition replaced: batch_ids=%s objects=%d row_count=%d chunks=%d",
        list(prepared.batch_ids),
        prepared.object_count,
        len(prepared.rows),
        len(chunks),
    )
    return LoadResult(
        spec.source_key,
        prepared.batch_ids,
        logical_date,
        len(prepared.rows),
        "loaded",
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
    expected = (prepared.expected_row_count, prepared.expected_row_count)
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
    watermark = None if spec.kind == "file" else read_watermark(executor, spec, catalog, schema)
    # Files may arrive late for a date older than the table watermark, so scan
    # their bounded manifest/archive registry and let coordinate checks skip
    # complete chunks. Other sources retain the established watermark path.
    pending = [
        day
        for day in discover_archive_dates(storage, spec)
        if spec.kind == "file" or watermark is None or day >= watermark
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
                    prepared.batch_ids,
                    logical_date,
                    len(prepared.rows),
                    "loaded",
                )
            )
    return results
