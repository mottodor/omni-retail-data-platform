"""Gold -> ClickHouse mart publisher: full-snapshot staging swap (ADR 0004).

Publication cycle per mart (mode = full snapshot swap, guide §26.3): read
the whole mart snapshot from Trino -> ``TRUNCATE`` the ``_staging`` twin ->
insert the snapshot into staging -> atomically ``EXCHANGE TABLES``. The
serving table is never written to directly, so BI observes either the
previous or the new snapshot, never a partial one; a failure at any step
leaves the previous serving data intact and recovery is a plain rerun.
Re-publishing identical Gold state yields an identical table — routine
refresh, retry after failure, and rebuild share this one code path.
"""

import logging
import time
from dataclasses import dataclass
from typing import Literal

from omni_retail.ingestion.common.logging import context_logger
from omni_retail.lakehouse.bronze.loader import TrinoExecutor
from omni_retail.serving.clickhouse.client import ClickHouseExecutor
from omni_retail.serving.clickhouse.specs import MartSpec

logger = logging.getLogger(__name__)


class PublishError(Exception):
    """Explicit serving publication failure (row-count mismatch, swap fault)."""


@dataclass(frozen=True)
class PublishResult:
    """Measurable outcome of one mart publication."""

    mart: str
    mode: Literal["publish", "rebuild"]
    row_count: int
    written_bytes: int
    duration_seconds: float


def snapshot_sql(spec: MartSpec, catalog: str) -> str:
    """Full-mart read from Gold: explicit columns, never ``select *``."""
    columns = ", ".join(f'"{name}"' for name in spec.column_names)
    return f"select {columns} from {catalog}.{spec.source_schema}.{spec.name}"


def truncate_staging_sql(spec: MartSpec) -> str:
    return f"truncate table {spec.staging_table}"


def exchange_tables_sql(spec: MartSpec) -> str:
    return f"exchange tables {spec.serving_table} and {spec.staging_table}"


def publish(
    trino_executor: TrinoExecutor, ch_executor: ClickHouseExecutor, spec: MartSpec
) -> PublishResult:
    """Swap in a fresh full snapshot of the mart (idempotent by construction)."""
    return _publish(trino_executor, ch_executor, spec, mode="publish")


def rebuild(
    trino_executor: TrinoExecutor, ch_executor: ClickHouseExecutor, spec: MartSpec
) -> PublishResult:
    """Recreate the mart DDL if it was dropped, then run the same publish.

    The "rebuildable from Iceberg Gold" acceptance criterion: after a
    ``DROP TABLE`` the spec DDL recreates the serving + staging pair and
    the standard swap path refills it — no separate backfill logic.
    """
    ch_executor.command(spec.serving_create_sql)
    ch_executor.command(spec.staging_create_sql)
    return _publish(trino_executor, ch_executor, spec, mode="rebuild")


def _publish(
    trino_executor: TrinoExecutor,
    ch_executor: ClickHouseExecutor,
    spec: MartSpec,
    *,
    mode: Literal["publish", "rebuild"],
) -> PublishResult:
    log = context_logger(__name__, mart=spec.name, mode=mode)
    started = time.monotonic()

    rows = trino_executor.fetch(snapshot_sql(spec, "iceberg"))
    ch_executor.command(truncate_staging_sql(spec))
    summary = ch_executor.insert(spec.staging_table, spec.column_names, rows)
    if summary.written_rows != len(rows):
        raise PublishError(
            f"{spec.name}: written row count mismatch: expected={len(rows)} "
            f"written={summary.written_rows} (serving table not modified)"
        )
    ch_executor.command(exchange_tables_sql(spec))

    duration = time.monotonic() - started
    log.info(
        "serving publication completed: rows=%d bytes=%d duration_seconds=%.3f target=%s",
        summary.written_rows,
        summary.written_bytes,
        duration,
        spec.serving_table,
    )
    return PublishResult(
        mart=spec.name,
        mode=mode,
        row_count=summary.written_rows,
        written_bytes=summary.written_bytes,
        duration_seconds=duration,
    )
