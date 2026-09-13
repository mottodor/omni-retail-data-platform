"""Factory for the API ingestion DAGs (Phase 4 design spec §4).

All three mock-API sources (fx-rates, marketing-campaigns, deliveries) share
one shape: a single TaskFlow ``ingest`` task in the ``mock_api`` pool that
returns the batch summary to XCom.
"""

from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import DAG

from include.runners import run_api_ingestion

#: Shared task policy (Phase 4 design spec §4): bounded retries with
#: exponential backoff on top of the http-level retries inside ApiClient.
#: (The delay cap argument is Airflow's ``max_retry_delay``.)
API_INGESTION_DEFAULT_ARGS: dict[str, object] = {
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=5),
}


def build_api_ingestion_dag(
    *,
    dag_id: str,
    source_name: str,
    start_date: datetime,
    schedule: str = "@daily",
    pool: str = "mock_api",
) -> DAG:
    """Create one API-ingestion DAG: ``ingest(ds)`` -> summary dict via XCom."""

    @dag(
        dag_id=dag_id,
        schedule=schedule,
        start_date=start_date,
        catchup=False,
        max_active_runs=1,
        default_args=API_INGESTION_DEFAULT_ARGS,
        tags=["ingestion", "api", source_name],
        doc_md=(
            f"Ingest one logical date of ``{source_name}`` from the mock API into the "
            "raw archive bucket (durable raw pages + batch manifest). "
            "Re-runs of the same logical date overwrite the same deterministic "
            "object keys and batch_id, so retries and backfills never duplicate data."
        ),
    )
    def generated() -> None:
        @task(pool=pool)
        def ingest(ds: str | None = None) -> dict[str, object]:
            if ds is None:
                raise ValueError("expected Airflow to inject the logical date as ds")
            return run_api_ingestion(source_name, date.fromisoformat(ds))

        ingest()

    return generated()
