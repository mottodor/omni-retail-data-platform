"""Airflow DAG: bounded weekly snapshot expiration for batch Bronze tables."""

from datetime import UTC, datetime

from airflow.decorators import dag as airflow_dag
from airflow.decorators import task
from include.policy import SNAPSHOT_MAINTENANCE_TASK_DEFAULT_ARGS
from include.runners import run_snapshot_maintenance

from omni_retail.lakehouse.bronze.specs import TABLES


@airflow_dag(
    dag_id="maintain_iceberg_snapshots",
    schedule="0 3 * * 0",
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=dict(SNAPSHOT_MAINTENANCE_TASK_DEFAULT_ARGS),
    tags=["lakehouse", "maintenance", "bronze"],
    doc_md=(
        "Weekly expiration of old snapshots for the fourteen registered batch "
        "Bronze tables. Each table is an independent task; the one-slot "
        "`iceberg_bronze` pool serializes expiration with `load_bronze`. "
        "Retention settings come from the environment and enforce repository "
        "safety floors. CDC and dbt-managed tables are outside this DAG."
    ),
)
def build_maintain_iceberg_snapshots_dag() -> None:
    @task(pool="iceberg_bronze")
    def expire_table(table_name: str) -> dict[str, object]:
        return run_snapshot_maintenance(table_name)

    for table_name in TABLES:
        expire_table.override(task_id=f"expire_{table_name}")(table_name)


dag = build_maintain_iceberg_snapshots_dag()
