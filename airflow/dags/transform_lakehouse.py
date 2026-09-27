"""Airflow DAG: full dbt build (Silver/Gold/marts) over Iceberg Bronze."""

from datetime import UTC, datetime

from airflow.decorators import dag, task
from include.datasets import BRONZE, GOLD
from include.policy import TRANSFORM_TASK_DEFAULT_ARGS
from include.runners import run_dbt_build


@dag(
    dag_id="transform_lakehouse",
    schedule=[BRONZE],
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=TRANSFORM_TASK_DEFAULT_ARGS,
    tags=["lakehouse", "dbt"],
    doc_md=(
        "Triggered by the `lakehouse://bronze` dataset emitted by "
        "`load_bronze`. Runs the full `dbt build` (staging -> intermediate -> "
        "core -> marts, plus built-in and singular tests) via the dbt CLI in "
        "the worker process — transformation SQL lives in the dbt project, "
        "not in this DAG (AGENTS.md §19.1). dbt target/log artifacts go to a "
        "per-run temp directory because the project dir is mounted read-only. "
        "`max_active_runs=1` keeps concurrent dbt builds off the single-node "
        "Trino/Polaris stack."
    ),
)
def build_transform_lakehouse_dag() -> None:
    @task(outlets=[GOLD])
    def dbt_build() -> dict[str, object]:
        return run_dbt_build()

    dbt_build()


dag = build_transform_lakehouse_dag()
