"""Keyset extraction of one OLTP table into a durable Parquet snapshot.

Semantics (Phase 4 design spec §5-§7):
- no watermark -> full snapshot (bootstrap); otherwise only rows with
  ``(updated_at, pk)`` strictly greater than the watermark (keyset ordering
  makes same-second events safe without any overlap parameter);
- streaming happens through a server-side cursor with bounded ``fetchmany``
  batches, so memory stays flat regardless of table size;
- output object ``archive/postgres/<table>/<yyyy>/<mm>/<dd>/data.parquet`` and
  ``batch_id = postgres-<table>-<yyyymmdd>`` are deterministic for a logical
  date: a re-run overwrites the same object instead of duplicating;
- the watermark advances only after a successful upload + manifest write;
- an empty increment writes only a ``completed`` manifest (row_count=0), no
  Parquet object and no watermark change.

Limitations (documented, closed by CDC in Phase 8): hard deletes are
invisible to snapshots, and history is not preserved — each object holds the
current state of its extraction window.
"""

import hashlib
import logging
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Protocol, cast

import pyarrow as pa
import pyarrow.parquet as pq

from omni_retail.ingestion.common.logging import context_logger
from omni_retail.ingestion.common.manifest import BatchManifest
from omni_retail.ingestion.common.paths import (
    BUCKET_ARCHIVE,
    manifest_key,
    postgres_snapshot_key,
)
from omni_retail.ingestion.common.storage import ObjectStorage
from omni_retail.ingestion.postgres_snapshot.tables import UPDATED_AT, TableSpec
from omni_retail.ingestion.postgres_snapshot.watermark import Watermark, save_watermark

Clock = Callable[[], datetime]
Row = tuple[object, ...]


class SnapshotCursor(Protocol):
    """Server-side cursor boundary (psycopg named cursor or a test fake)."""

    itersize: int

    def execute(self, query: str, params: tuple[object, ...]) -> None: ...

    def fetchmany(self, size: int) -> list[Row]: ...

    def __enter__(self) -> "SnapshotCursor": ...

    def __exit__(self, *exc_info: object) -> None: ...


class SnapshotConnection(Protocol):
    """Connection boundary required by the extraction (real psycopg or fake)."""

    def cursor(self, name: str | None = None) -> SnapshotCursor: ...


logger = logging.getLogger(__name__)

#: Server-side cursor batch size (rows per fetch round-trip).
FETCH_SIZE = 10_000


def build_select(spec: TableSpec, watermark: Watermark | None) -> tuple[str, tuple[object, ...]]:
    """Keyset SELECT for the table; identifiers come from the declarative spec."""
    columns = ", ".join(f'"{column.name}"' for column in spec.columns)
    order_by = f'order by "{UPDATED_AT}", "{spec.pk}"'
    if watermark is None:
        return f'select {columns} from "{spec.name}" {order_by}', ()
    where = f'where ("{UPDATED_AT}", "{spec.pk}") > (%s, %s)'
    return (
        f'select {columns} from "{spec.name}" {where} {order_by}',
        (watermark.updated_at, watermark.pk),
    )


def extract_rows(
    conn: SnapshotConnection,
    spec: TableSpec,
    watermark: Watermark | None,
    fetch_size: int = FETCH_SIZE,
) -> list[Row]:
    """Stream every row of the window through a server-side cursor."""
    sql, params = build_select(spec, watermark)
    rows: list[Row] = []
    with conn.cursor(name=f"snapshot_{spec.name}") as cursor:
        cursor.itersize = fetch_size
        cursor.execute(sql, params)
        while True:
            chunk = cursor.fetchmany(fetch_size)
            if not chunk:
                break
            rows.extend(chunk)
    return rows


def rows_to_parquet(spec: TableSpec, rows: list[Row]) -> bytes:
    """Serialize rows with the explicit Arrow schema of the table spec."""
    arrays = [
        pa.array([row[index] for row in rows], type=column.type)
        for index, column in enumerate(spec.columns)
    ]
    table = pa.Table.from_arrays(arrays, schema=spec.arrow_schema())
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="snappy")
    return cast(bytes, sink.getvalue().to_pybytes())


def snapshot_table(
    storage: ObjectStorage,
    conn: SnapshotConnection,
    spec: TableSpec,
    *,
    logical_date: date,
    watermark: Watermark | None = None,
    clock: Clock | None = None,
    fetch_size: int = FETCH_SIZE,
) -> BatchManifest:
    """Extract, upload and register one logical snapshot of the table."""
    effective_clock: Clock = clock or (lambda: datetime.now(UTC))
    log = context_logger(__name__, source=spec.source_name, logical_date=logical_date.isoformat())

    rows = extract_rows(conn, spec, watermark, fetch_size)
    object_key = postgres_snapshot_key(spec.name, logical_date)
    batch_id = f"{spec.batch_prefix}{logical_date:%Y%m%d}"
    body = rows_to_parquet(spec, rows) if rows else b""
    manifest = _build_manifest(
        spec=spec,
        batch_id=batch_id,
        logical_date=logical_date,
        object_key=object_key,
        row_count=len(rows),
        body=body,
        ingested_at=effective_clock(),
    )
    storage.put_object(
        BUCKET_ARCHIVE,
        manifest_key(spec.source_name, manifest.batch_id),
        manifest.to_json().encode(),
    )

    if not rows:
        log.info("postgres snapshot empty window: batch_id=%s row_count=0", manifest.batch_id)
        return manifest

    storage.put_object(BUCKET_ARCHIVE, object_key, body)
    save_watermark(
        storage,
        Watermark(
            table=spec.name,
            updated_at=cast(datetime, rows[-1][spec.column_index(UPDATED_AT)]),
            pk=int(cast(int, rows[-1][spec.column_index(spec.pk)])),
        ),
    )
    log.info(
        "postgres snapshot completed: batch_id=%s row_count=%d object_key=%s",
        manifest.batch_id,
        manifest.row_count,
        manifest.object_key,
    )
    return manifest


def _build_manifest(
    *,
    spec: TableSpec,
    batch_id: str,
    logical_date: date,
    object_key: str,
    row_count: int,
    body: bytes,
    ingested_at: datetime,
) -> BatchManifest:
    return BatchManifest(
        batch_id=batch_id,
        source=spec.source_name,
        source_kind="postgres",
        status="completed",
        object_key=object_key,
        checksum=hashlib.sha256(body).hexdigest(),
        size_bytes=len(body),
        ingested_at=ingested_at,
        logical_date=logical_date,
        row_count=row_count,
        rejected_row_count=0,
        schema_version=spec.schema_version,
    )
