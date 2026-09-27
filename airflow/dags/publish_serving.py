"""Airflow DAG: publish the current Iceberg Gold snapshot to ClickHouse."""

from datetime import UTC, datetime

from airflow.decorators import dag, task
from include.datasets import GOLD
from include.policy import SERVING_TASK_DEFAULT_ARGS
from include.runners import run_serving_rebuild


@dag(
    dag_id="publish_serving",
    schedule=[GOLD],
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=SERVING_TASK_DEFAULT_ARGS,
    tags=["serving", "clickhouse"],
    doc_md=(
        "Triggered by the `lakehouse://gold` dataset emitted after a successful "
        "`transform_lakehouse` run. Rebuilds the derived ClickHouse serving "
        "marts from Iceberg Gold through the idempotent staging-swap publisher. "
        "Iceberg remains the source of truth; this DAG contains orchestration "
        "only and no transformation SQL."
    ),
)
def publish_serving_dag() -> None:
    @task
    def publish() -> dict[str, object]:
        return run_serving_rebuild()

    publish()


dag = publish_serving_dag()
