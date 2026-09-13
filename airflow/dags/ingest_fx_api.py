"""Airflow DAG: ingest one logical date of ``fx-rates`` from the mock API."""

from datetime import UTC, datetime

from include.api_dag_factory import build_api_ingestion_dag

dag = build_api_ingestion_dag(
    dag_id="ingest_fx_api",
    source_name="fx-rates",
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
)
