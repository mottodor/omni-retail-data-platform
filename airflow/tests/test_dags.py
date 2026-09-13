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
EXPECTED_DAG_IDS = {"ingest_fx_api"}


@pytest.fixture(scope="module")
def dag_bag() -> DagBag:
    return DagBag(dag_folder=DAG_FOLDER, include_examples=False)


def test_dag_bag_has_no_import_errors(dag_bag: DagBag) -> None:
    assert not dag_bag.import_errors, f"DAG import errors: {dag_bag.import_errors}"


def test_expected_dag_ids_are_registered(dag_bag: DagBag) -> None:
    assert set(dag_bag.dags) == EXPECTED_DAG_IDS


def test_fx_api_dag_has_single_ingest_task(dag_bag: DagBag) -> None:
    dag = dag_bag.dags["ingest_fx_api"]
    assert [task.task_id for task in dag.tasks] == ["ingest"]
    assert not dag.get_task("ingest").upstream_task_ids
    assert not dag.get_task("ingest").downstream_task_ids


def test_fx_api_dag_schedule_policy(dag_bag: DagBag) -> None:
    dag = dag_bag.dags["ingest_fx_api"]
    assert dag.schedule_interval == "@daily"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.start_date == datetime(2026, 9, 1, tzinfo=UTC)


def test_fx_api_task_retry_and_timeout_policy(dag_bag: DagBag) -> None:
    task = dag_bag.dags["ingest_fx_api"].get_task("ingest")
    assert task.retries == 3
    assert task.retry_delay == timedelta(seconds=30)
    assert task.retry_exponential_backoff is True
    assert task.max_retry_delay == timedelta(minutes=5)
    assert task.execution_timeout == timedelta(minutes=5)


def test_fx_api_task_uses_mock_api_pool(dag_bag: DagBag) -> None:
    task = dag_bag.dags["ingest_fx_api"].get_task("ingest")
    assert task.pool == "mock_api"
