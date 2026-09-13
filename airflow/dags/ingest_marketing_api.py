"""Airflow DAG: ingest one logical date of ``marketing-campaigns`` from the mock API."""

from datetime import UTC, datetime

from include.api_dag_factory import build_api_ingestion_dag

dag = build_api_ingestion_dag(
    dag_id="ingest_marketing_api",
    source_name="marketing-campaigns",
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
)
