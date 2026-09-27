"""Airflow DAG: incremental Parquet snapshots of the seven OLTP tables.

Seven independent tasks — one per table. The first run without a watermark
bootstraps a full snapshot (~115k rows total, acceptable for one node);
subsequent runs extract only ``(updated_at, pk)`` windows past the durable
watermark in the archive bucket. ``full_refresh`` (DAG param, default
``False``) forces a full snapshot for a table bootstrap or a rebase
(Phase 4 design spec §4, §5).
"""

from collections.abc import Mapping
from datetime import UTC, date, datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from include.datasets import RAW_POSTGRES_SNAPSHOT
from include.policy import INGESTION_TASK_DEFAULT_ARGS
from include.runners import run_postgres_snapshot

SNAPSHOT_TABLES: tuple[str, ...] = (
    "categories",
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "shipments",
)


@dag(
    dag_id="ingest_postgres_snapshot",
    schedule="@daily",
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=INGESTION_TASK_DEFAULT_ARGS,
    params={
        "full_refresh": Param(
            default=False,
            type="boolean",
            title="Full refresh",
            description=(
                "When true, ignore the watermark and extract a full snapshot "
                "of every table (bootstrap/rebase); when false, extract only "
                "rows newer than the stored watermark."
            ),
        )
    },
    tags=["ingestion", "postgres"],
    doc_md=(
        "Snapshots hold the current state of the source; historical backfill "
        "by logical date is not possible for this source kind (no history in "
        "OLTP) and is closed by CDC in Phase 8. Re-runs of the same logical "
        "date overwrite the same deterministic object keys — no duplicates. "
        "Hard deletes are invisible to snapshots (documented limitation)."
    ),
)
def build_postgres_snapshot_dag() -> None:
    for table in SNAPSHOT_TABLES:

        @task(task_id=f"snapshot_{table}", outlets=[RAW_POSTGRES_SNAPSHOT])
        def snapshot(
            ds: str | None = None,
            params: Mapping[str, object] | None = None,
            table: str = table,
        ) -> dict[str, object]:
            if ds is None:
                raise ValueError("expected Airflow to inject the logical date as ds")
            effective_params: Mapping[str, object] = params or {}
            full_refresh = bool(effective_params.get("full_refresh", False))
            return run_postgres_snapshot(
                table,
                date.fromisoformat(ds),
                full_refresh=full_refresh,
            )

        snapshot()


dag = build_postgres_snapshot_dag()
