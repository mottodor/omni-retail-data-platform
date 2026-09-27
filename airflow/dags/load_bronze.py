"""Airflow DAG: watermark-driven Iceberg Bronze load of every source."""

from datetime import UTC, datetime

from airflow.decorators import dag, task
from include.datasets import BRONZE, BRONZE_RAW_INPUTS
from include.policy import LAKEHOUSE_TASK_DEFAULT_ARGS
from include.runners import run_bronze_load


@dag(
    dag_id="load_bronze",
    schedule=BRONZE_RAW_INPUTS,
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=LAKEHOUSE_TASK_DEFAULT_ARGS,
    tags=["lakehouse", "bronze"],
    doc_md=(
        "Triggered by the `raw://` datasets of the four Bronze-feeding "
        "ingestion DAGs (PostgreSQL snapshot + three API sources). The load "
        "is watermark-driven: per source, every archived logical date past "
        "`max(_batch_date)` loads in ascending order, so a dataset-triggered "
        "run never relies on its own logical date (which does not equal the "
        "producer's) and re-triggers between batches are idempotent no-ops. "
        "Supplier files reach the raw archive but are not loaded into Bronze "
        "yet (deferred follow-up), so `ingest_supplier_files` emits none of "
        "the consumed datasets."
    ),
)
def build_load_bronze_dag() -> None:
    @task(outlets=[BRONZE])
    def load_new() -> dict[str, object]:
        return run_bronze_load()

    load_new()


dag = build_load_bronze_dag()
