"""Thin orchestration runners over the Phase 3/4 ingestion functions.

DAG tasks must not become transformation code repositories (AGENTS.md
§19.1): the runners only wire environment-configured ingestion entry points
to Airflow task context and return JSON-serializable summaries for XCom.
"""

import contextlib
from datetime import date
from typing import cast

import psycopg

from omni_retail.ingestion.api.cli import ApiSourceSpec, run_batch, spec_by_name
from omni_retail.ingestion.api.client import ApiClient, ApiClientConfig
from omni_retail.ingestion.common.logging import configure_logging
from omni_retail.ingestion.common.manifest import BatchManifest
from omni_retail.ingestion.common.storage import BotoObjectStorage, StorageConfig
from omni_retail.ingestion.files.flow import BatchOutcome, process_incoming
from omni_retail.ingestion.files.schemas import schema_by_name as file_schema_by_name
from omni_retail.ingestion.postgres_snapshot.config import PostgresSourceConfig
from omni_retail.ingestion.postgres_snapshot.extract import SnapshotConnection, snapshot_table
from omni_retail.ingestion.postgres_snapshot.tables import table_by_name
from omni_retail.ingestion.postgres_snapshot.watermark import load_watermark


def summarize_manifest(manifest: BatchManifest) -> dict[str, object]:
    """XCom-friendly batch summary: identifiers and counts only."""
    return {
        "batch_id": manifest.batch_id,
        "status": manifest.status,
        "row_count": manifest.row_count,
        "object_key": manifest.object_key,
    }


def run_api_ingestion(source_name: str, logical_date: date) -> dict[str, object]:
    """Fetch and persist one logical batch of an API source (Phase 3 pipeline)."""
    configure_logging()
    spec: ApiSourceSpec = spec_by_name(source_name)
    client = ApiClient(ApiClientConfig.from_env())
    with contextlib.closing(client):
        storage = BotoObjectStorage(StorageConfig.from_env())
        manifest = run_batch(spec, client, storage, logical_date, page_size=None)
    return summarize_manifest(manifest)


def summarize_outcomes(outcomes: tuple[BatchOutcome, ...]) -> list[dict[str, object]]:
    """XCom-friendly summaries of processed files (identifiers and counts only)."""
    return [
        {
            "filename": outcome.filename,
            "batch_id": outcome.manifest.batch_id,
            "status": outcome.manifest.status,
            "row_count": outcome.manifest.row_count,
            "rejected_row_count": outcome.manifest.rejected_row_count,
        }
        for outcome in outcomes
    ]


def run_file_ingestion(
    source_name: str, run_date: date, *, fail_on_rejected: bool = False
) -> list[dict[str, object]]:
    """Process pending vendor files of one source through the Phase 3 flow."""
    configure_logging()
    schema = file_schema_by_name(source_name)
    storage = BotoObjectStorage(StorageConfig.from_env())
    outcomes = process_incoming(
        storage,
        schema,
        run_date,
        fail_on_rejected=fail_on_rejected,
    )
    return summarize_outcomes(outcomes)


def run_postgres_snapshot(
    table_name: str, logical_date: date, *, full_refresh: bool = False
) -> dict[str, object]:
    """Extract one OLTP table snapshot into the archive bucket (Phase 4 §5)."""
    configure_logging()
    spec = table_by_name(table_name)
    config = PostgresSourceConfig.from_env()
    storage = BotoObjectStorage(StorageConfig.from_env())
    with psycopg.connect(config.conninfo()) as conn:
        watermark = None if full_refresh else load_watermark(storage, spec)
        manifest = snapshot_table(
            storage,
            cast(SnapshotConnection, conn),
            spec,
            logical_date=logical_date,
            watermark=watermark,
        )
    return summarize_manifest(manifest)
