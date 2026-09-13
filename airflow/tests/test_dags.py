"""DAG import/structure tests (Phase 4 design spec §12).

These tests run *inside* the custom Airflow image (`make airflow-test`,
`infrastructure/scripts/airflow_tests.sh`); Airflow is not installed in the
dev venv. The DagBag harness enforces AGENTS.md §19.4: no import errors,
expected dag_ids, task structure, retry/timeout/pool policy.
"""

from datetime import UTC, datetime, timedelta

import pytest
from airflow.models import DagBag

DAG_FOLDER = "/opt/airflow/dags"

#: Registry of DAGs expected in this phase (grows with Phase 4 slices).
EXPECTED_DAG_IDS = {
    "ingest_fx_api",
    "ingest_marketing_api",
    "ingest_delivery_api",
    "ingest_supplier_files",
    "ingest_postgres_snapshot",
}

API_DAG_IDS = sorted(
    dag_id
    for dag_id in EXPECTED_DAG_IDS
    if dag_id.startswith("ingest_") and dag_id.endswith("_api")
)

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


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
def test_dag_schedule_policy(dag_bag: DagBag, dag_id: str) -> None:
    dag = dag_bag.dags[dag_id]
    assert dag.schedule_interval == "@daily"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.start_date == datetime(2026, 9, 1, tzinfo=UTC)


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
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
