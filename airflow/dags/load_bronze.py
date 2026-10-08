"""Airflow DAG: watermark-driven Iceberg Bronze load of every source."""

from datetime import UTC, datetime

from airflow.decorators import dag as airflow_dag
from airflow.decorators import task
from include.datasets import BRONZE, BRONZE_RAW_INPUTS
from include.policy import LAKEHOUSE_TASK_DEFAULT_ARGS
from include.runners import run_bronze_load


@airflow_dag(
    dag_id="load_bronze",
    schedule=BRONZE_RAW_INPUTS,
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=dict(LAKEHOUSE_TASK_DEFAULT_ARGS),
    tags=["lakehouse", "bronze"],
    doc_md=(
        "Triggered by the `raw://` datasets of the Bronze-feeding ingestion "
        "DAGs (PostgreSQL snapshots, three APIs, and four file sources). "
        "PostgreSQL/API loading resumes from each table's date watermark; file "
        "loading scans completed manifests so late files for older dates remain "
        "eligible. Source-object coordinate checks make repeated triggers and "
        "ambiguous commit recovery idempotent."
    ),
)
def build_load_bronze_dag() -> None:
    @task(outlets=[BRONZE])
    def load_new() -> dict[str, object]:
        return run_bronze_load()

    load_new()


dag = build_load_bronze_dag()
