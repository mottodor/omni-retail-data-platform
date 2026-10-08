"""Airflow DAG: bounded weekly maintenance of the raw CDC Iceberg ledger."""

from datetime import UTC, datetime

from airflow.decorators import dag as airflow_dag
from airflow.decorators import task
from include.policy import (
    CDC_COMPACTION_TASK_DEFAULT_ARGS,
    CDC_EXPIRATION_TASK_DEFAULT_ARGS,
)
from include.runners import run_cdc_compaction, run_cdc_snapshot_expiration


@airflow_dag(
    dag_id="maintain_cdc_iceberg",
    schedule="30 4 * * 0",
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "maintenance", "bronze", "cdc"],
    doc_md=(
        "Weekly data-file compaction followed by protected snapshot expiration "
        "for `bronze.postgres_cdc_events`. Both tasks use the one-slot "
        "`iceberg_bronze` pool, which serializes Airflow-managed Bronze work but "
        "does not stop or lock the external long-running CDC consumer. Iceberg "
        "optimistic commits and bounded task retries handle commit conflicts."
    ),
)
def build_maintain_cdc_iceberg_dag() -> None:
    @task(pool="iceberg_bronze", **CDC_COMPACTION_TASK_DEFAULT_ARGS)
    def compact_data_files() -> dict[str, object]:
        return run_cdc_compaction()

    @task(pool="iceberg_bronze", **CDC_EXPIRATION_TASK_DEFAULT_ARGS)
    def expire_snapshots() -> dict[str, object]:
        return run_cdc_snapshot_expiration()

    compact_task = compact_data_files()
    expire_task = expire_snapshots()
    compact_task.set_downstream(expire_task)


dag = build_maintain_cdc_iceberg_dag()
