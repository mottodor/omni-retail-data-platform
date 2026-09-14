"""Airflow DAG: process pending vendor files of all four file sources.

Four independent tasks — one per source. A failure of one source (bad file,
quarantine in strict mode) never stops the others; each task reports its
processed files to XCom. ``fail_on_rejected`` (DAG param, default ``False``)
turns quarantined data into a task failure instead of a logged warning
(Phase 4 design spec §4, §7).
"""

from collections.abc import Mapping
from datetime import UTC, date, datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from include.policy import INGESTION_TASK_DEFAULT_ARGS
from include.runners import run_file_ingestion

FILE_SOURCES: tuple[str, ...] = (
    "supplier-prices",
    "partner-products",
    "historical-orders",
    "supplier-stock",
)


@dag(
    dag_id="ingest_supplier_files",
    schedule="@daily",
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=INGESTION_TASK_DEFAULT_ARGS,
    params={
        "fail_on_rejected": Param(
            default=False,
            type="boolean",
            title="Fail on quarantined data",
            description=(
                "When true, a task fails if any file was rejected or any row "
                "was quarantined; when false, quarantine is logged and the "
                "task succeeds."
            ),
        )
    },
    tags=["ingestion", "files"],
    doc_md=(
        "Processing follows the Phase 3 file flow: landing -> processing -> "
        "archive | rejected, with duplicate detection, manifests and "
        "quarantine reasons. Re-runs are idempotent: duplicate files are "
        "detected via checksum markers and skipped."
    ),
)
def build_supplier_files_dag() -> None:
    for source in FILE_SOURCES:

        @task(task_id=f"ingest_{source.replace('-', '_')}")
        def ingest(
            ds: str | None = None,
            params: Mapping[str, object] | None = None,
            source: str = source,
        ) -> list[dict[str, object]]:
            if ds is None:
                raise ValueError("expected Airflow to inject the logical date as ds")
            effective_params: Mapping[str, object] = params or {}
            fail_on_rejected = bool(effective_params.get("fail_on_rejected", False))
            return run_file_ingestion(
                source,
                date.fromisoformat(ds),
                fail_on_rejected=fail_on_rejected,
            )

        ingest()


dag = build_supplier_files_dag()
