"""Airflow DAG: stable-boundary CDC build and atomic serving publication."""

from collections.abc import Mapping
from datetime import UTC, datetime

from airflow.decorators import dag, task
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from include.datasets import BRONZE
from include.policy import (
    CDC_BOUNDARY_TASK_DEFAULT_ARGS,
    SERVING_TASK_DEFAULT_ARGS,
    TRANSFORM_TASK_DEFAULT_ARGS,
)
from include.runners import run_cdc_boundary, run_dbt_build, run_serving_rebuild


@dag(
    dag_id="transform_lakehouse",
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 * * * *", timezone="UTC"),
        datasets=BRONZE,
    ),
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "dbt", "cdc", "serving"],
    doc_md=(
        "Runs after a `lakehouse://bronze` dataset update or hourly at minute "
        "zero. Captures a healthy, quiescent CDC consumer boundary, passes its "
        "exclusive Kafka offsets to the full `dbt build`, and only then "
        "atomically republishes ClickHouse marts. `max_active_runs=1` serializes "
        "the complete boundary/build/publication chain. The DAG starts paused; "
        "operators must complete the initial-snapshot gate in the CDC runbook "
        "before first unpause. Transformation SQL remains in dbt."
    ),
)
def build_transform_lakehouse_dag() -> None:
    @task(**CDC_BOUNDARY_TASK_DEFAULT_ARGS)
    def wait_for_cdc_boundary() -> dict[str, dict[str, int]]:
        return run_cdc_boundary()

    @task(**TRANSFORM_TASK_DEFAULT_ARGS)
    def dbt_build(boundary: object) -> dict[str, object]:
        if not isinstance(boundary, Mapping):
            raise TypeError("CDC boundary XCom value must be a mapping")
        return run_dbt_build(boundary)

    @task(**SERVING_TASK_DEFAULT_ARGS)
    def publish_serving() -> dict[str, object]:
        return run_serving_rebuild()

    boundary = wait_for_cdc_boundary()
    build = dbt_build(boundary)
    publish = publish_serving()
    _ = build >> publish


dag = build_transform_lakehouse_dag()
