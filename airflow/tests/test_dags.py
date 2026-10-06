"""DAG import/structure tests (Phase 4 design spec §12).

These tests run *inside* the custom Airflow image (`make airflow-test`,
`infrastructure/scripts/airflow_tests.sh`); Airflow is not installed in the
dev venv. The DagBag harness enforces AGENTS.md §19.4: no import errors,
expected dag_ids, task structure, retry/timeout/pool policy.
"""

from datetime import UTC, datetime, timedelta

import pytest
from airflow.datasets import Dataset
from airflow.models import DagBag
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.simple import DatasetTriggeredTimetable
from airflow.timetables.trigger import CronTriggerTimetable

DAG_FOLDER = "/opt/airflow/dags"

#: Registry of DAGs expected in this phase (grows with Phase 4/5 slices).
EXPECTED_DAG_IDS = {
    "ingest_fx_api",
    "ingest_marketing_api",
    "ingest_delivery_api",
    "ingest_supplier_files",
    "ingest_postgres_snapshot",
    "load_bronze",
    "transform_lakehouse",
}

INGESTION_DAG_IDS = {
    "ingest_fx_api",
    "ingest_marketing_api",
    "ingest_delivery_api",
    "ingest_supplier_files",
    "ingest_postgres_snapshot",
}

LAKEHOUSE_DAG_IDS = {"load_bronze", "transform_lakehouse"}

#: dag_id -> raw dataset URI emitted by the API ingestion task.
API_DAG_DATASET_URIS = {
    "ingest_fx_api": "raw://fx-rates",
    "ingest_marketing_api": "raw://marketing-campaigns",
    "ingest_delivery_api": "raw://deliveries",
}

API_DAG_IDS = sorted(API_DAG_DATASET_URIS)

EXPECTED_SUPPLIER_TASK_IDS = {
    "ingest_supplier_prices",
    "ingest_partner_products",
    "ingest_historical_orders",
    "ingest_supplier_stock",
}

EXPECTED_SNAPSHOT_TASK_IDS = {
    "snapshot_categories",
    "snapshot_customers",
    "snapshot_products",
    "snapshot_orders",
    "snapshot_order_items",
    "snapshot_payments",
    "snapshot_shipments",
}


@pytest.fixture(scope="module")
def dag_bag() -> DagBag:
    return DagBag(dag_folder=DAG_FOLDER, include_examples=False)


def test_dag_bag_has_no_import_errors(dag_bag: DagBag) -> None:
    assert not dag_bag.import_errors, f"DAG import errors: {dag_bag.import_errors}"


def test_expected_dag_ids_are_registered(dag_bag: DagBag) -> None:
    assert set(dag_bag.dags) == EXPECTED_DAG_IDS


@pytest.mark.parametrize("dag_id", sorted(INGESTION_DAG_IDS))
def test_dag_schedule_policy(dag_bag: DagBag, dag_id: str) -> None:
    dag = dag_bag.dags[dag_id]
    assert dag.schedule_interval == "@daily"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.start_date == datetime(2026, 9, 1, tzinfo=UTC)


@pytest.mark.parametrize("dag_id", sorted(LAKEHOUSE_DAG_IDS))
def test_lakehouse_dag_common_policy(dag_bag: DagBag, dag_id: str) -> None:
    dag = dag_bag.dags[dag_id]
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.start_date == datetime(2026, 9, 1, tzinfo=UTC)


def dataset_uris(dag_bag: DagBag, dag_id: str) -> set[str]:
    """URIs of the datasets a dataset-scheduled DAG is triggered by."""
    timetable = dag_bag.dags[dag_id].timetable
    assert isinstance(timetable, DatasetTriggeredTimetable), f"{dag_id} is not dataset-scheduled"
    # Airflow 2.11: iter_datasets() yields (name, Dataset) tuples.
    return {name for name, _dataset in timetable.dataset_condition.iter_datasets()}


def test_load_bronze_is_triggered_by_raw_datasets(dag_bag: DagBag) -> None:
    # Expected URIs go through Dataset() because Airflow 2.11 normalizes
    # URIs (e.g. appends a trailing slash) inside the constructor.
    assert dataset_uris(dag_bag, "load_bronze") == {
        Dataset(uri).uri
        for uri in (
            "raw://postgres-snapshot",
            "raw://fx-rates",
            "raw://marketing-campaigns",
            "raw://deliveries",
        )
    }


def test_transform_lakehouse_uses_dataset_or_hourly_schedule(dag_bag: DagBag) -> None:
    timetable = dag_bag.dags["transform_lakehouse"].timetable
    assert isinstance(timetable, DatasetOrTimeSchedule)
    assert isinstance(timetable.timetable, CronTriggerTimetable)
    assert timetable.timetable.serialize() == {
        "expression": "0 * * * *",
        "timezone": "UTC",
        "interval": 0.0,
    }
    assert {name for name, _dataset in timetable.dataset_condition.iter_datasets()} == {
        Dataset("lakehouse://bronze").uri
    }


@pytest.mark.parametrize("dag_id", sorted(INGESTION_DAG_IDS))
def test_dag_task_retry_and_timeout_policy(dag_bag: DagBag, dag_id: str) -> None:
    tasks = dag_bag.dags[dag_id].tasks
    assert tasks, f"{dag_id} has no tasks"
    for task in tasks:
        assert task.retries == 3, f"{dag_id}.{task.task_id}: retries"
        assert task.retry_delay == timedelta(seconds=30), f"{dag_id}.{task.task_id}: delay"
        assert task.retry_exponential_backoff is True, f"{dag_id}.{task.task_id}: backoff"
        assert task.max_retry_delay == timedelta(minutes=5), f"{dag_id}.{task.task_id}: cap"
        assert task.execution_timeout == timedelta(minutes=5), f"{dag_id}.{task.task_id}: timeout"


@pytest.mark.parametrize("dag_id", API_DAG_IDS)
def test_api_dag_has_single_ingest_task(dag_bag: DagBag, dag_id: str) -> None:
    dag = dag_bag.dags[dag_id]
    assert [task.task_id for task in dag.tasks] == ["ingest"]
    assert not dag.get_task("ingest").upstream_task_ids
    assert not dag.get_task("ingest").downstream_task_ids


@pytest.mark.parametrize("dag_id", API_DAG_IDS)
def test_api_dag_task_uses_mock_api_pool(dag_bag: DagBag, dag_id: str) -> None:
    task = dag_bag.dags[dag_id].get_task("ingest")
    assert task.pool == "mock_api"


def test_supplier_files_dag_has_one_task_per_source(dag_bag: DagBag) -> None:
    dag = dag_bag.dags["ingest_supplier_files"]
    assert {task.task_id for task in dag.tasks} == EXPECTED_SUPPLIER_TASK_IDS


def test_supplier_files_tasks_are_independent(dag_bag: DagBag) -> None:
    """A failure of one source must not stop the others (design spec §7)."""
    dag = dag_bag.dags["ingest_supplier_files"]
    for task in dag.tasks:
        assert not task.upstream_task_ids, f"{task.task_id} must not depend on other sources"
        assert not task.downstream_task_ids, f"{task.task_id} must not be a dependency"


def test_supplier_files_tasks_use_default_pool(dag_bag: DagBag) -> None:
    dag = dag_bag.dags["ingest_supplier_files"]
    for task in dag.tasks:
        assert task.pool == "default_pool", (
            f"{task.task_id}: file sources do not touch the mock API pool"
        )


def test_supplier_files_dag_params_defaults(dag_bag: DagBag) -> None:
    dag = dag_bag.dags["ingest_supplier_files"]
    params = dag.params
    assert params["fail_on_rejected"] is False


def test_postgres_snapshot_dag_has_one_task_per_table(dag_bag: DagBag) -> None:
    dag = dag_bag.dags["ingest_postgres_snapshot"]
    assert {task.task_id for task in dag.tasks} == EXPECTED_SNAPSHOT_TASK_IDS


def test_postgres_snapshot_tasks_are_independent(dag_bag: DagBag) -> None:
    """A failure of one table must not stop the others (design spec §7)."""
    dag = dag_bag.dags["ingest_postgres_snapshot"]
    for task in dag.tasks:
        assert not task.upstream_task_ids, f"{task.task_id} must not depend on other tables"
        assert not task.downstream_task_ids, f"{task.task_id} must not be a dependency"


def test_postgres_snapshot_tasks_use_default_pool(dag_bag: DagBag) -> None:
    """Seven concurrent PostgreSQL connections are fine; no pool needed."""
    dag = dag_bag.dags["ingest_postgres_snapshot"]
    for task in dag.tasks:
        assert task.pool == "default_pool"


def test_postgres_snapshot_dag_params_defaults(dag_bag: DagBag) -> None:
    dag = dag_bag.dags["ingest_postgres_snapshot"]
    assert dag.params["full_refresh"] is False


def test_load_bronze_task_policy(dag_bag: DagBag) -> None:
    task = dag_bag.dags["load_bronze"].get_task("load_new")
    assert task.retries == 2
    assert task.retry_delay == timedelta(minutes=1)
    assert task.retry_exponential_backoff is True
    assert task.max_retry_delay == timedelta(minutes=10)
    assert task.execution_timeout == timedelta(minutes=30)


@pytest.mark.parametrize(
    ("task_id", "retries", "delay", "cap", "timeout"),
    [
        (
            "wait_for_cdc_boundary",
            3,
            timedelta(minutes=1),
            timedelta(minutes=10),
            timedelta(minutes=6),
        ),
        (
            "dbt_build",
            2,
            timedelta(minutes=5),
            timedelta(minutes=30),
            timedelta(minutes=60),
        ),
        (
            "publish_serving",
            2,
            timedelta(minutes=2),
            timedelta(minutes=15),
            timedelta(minutes=30),
        ),
    ],
)
def test_coordinated_refresh_task_policy(
    dag_bag: DagBag,
    task_id: str,
    retries: int,
    delay: timedelta,
    cap: timedelta,
    timeout: timedelta,
) -> None:
    task = dag_bag.dags["transform_lakehouse"].get_task(task_id)
    assert task.retries == retries
    assert task.retry_delay == delay
    assert task.retry_exponential_backoff is True
    assert task.max_retry_delay == cap
    assert task.execution_timeout == timeout


def test_lakehouse_dag_task_graph(dag_bag: DagBag) -> None:
    load_tasks = dag_bag.dags["load_bronze"].tasks
    assert [task.task_id for task in load_tasks] == ["load_new"]

    dag = dag_bag.dags["transform_lakehouse"]
    assert {task.task_id for task in dag.tasks} == {
        "wait_for_cdc_boundary",
        "dbt_build",
        "publish_serving",
    }
    assert dag.get_task("wait_for_cdc_boundary").downstream_task_ids == {"dbt_build"}
    assert dag.get_task("dbt_build").upstream_task_ids == {"wait_for_cdc_boundary"}
    assert dag.get_task("dbt_build").downstream_task_ids == {"publish_serving"}
    assert dag.get_task("publish_serving").upstream_task_ids == {"dbt_build"}


def test_lakehouse_tasks_use_default_pool(dag_bag: DagBag) -> None:
    """Lakehouse tasks do not touch the mock API pool."""
    for dag_id in sorted(LAKEHOUSE_DAG_IDS):
        for task in dag_bag.dags[dag_id].tasks:
            assert task.pool == "default_pool", f"{dag_id}.{task.task_id}: pool"


@pytest.mark.parametrize("dag_id", API_DAG_IDS)
def test_api_dag_task_emits_raw_dataset(dag_bag: DagBag, dag_id: str) -> None:
    task = dag_bag.dags[dag_id].get_task("ingest")
    assert task.outlets == [Dataset(API_DAG_DATASET_URIS[dag_id])]


def test_postgres_snapshot_tasks_emit_raw_dataset(dag_bag: DagBag) -> None:
    dag = dag_bag.dags["ingest_postgres_snapshot"]
    for task in dag.tasks:
        assert task.outlets == [Dataset("raw://postgres-snapshot")], task.task_id


def test_supplier_files_emit_no_dataset_yet(dag_bag: DagBag) -> None:
    """File sources are not loaded into Bronze yet (deferred follow-up)."""
    dag = dag_bag.dags["ingest_supplier_files"]
    for task in dag.tasks:
        assert not task.outlets, task.task_id


def test_load_bronze_task_emits_bronze_dataset(dag_bag: DagBag) -> None:
    task = dag_bag.dags["load_bronze"].get_task("load_new")
    assert task.outlets == [Dataset("lakehouse://bronze")]


def test_coordinated_refresh_emits_no_intermediate_gold_dataset(dag_bag: DagBag) -> None:
    dag = dag_bag.dags["transform_lakehouse"]
    assert all(not task.outlets for task in dag.tasks)
