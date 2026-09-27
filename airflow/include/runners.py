"""Thin orchestration runners over the Phase 3/4/5 pipeline functions.

DAG tasks must not become transformation code repositories (AGENTS.md
§19.1): the runners only wire environment-configured entry points to Airflow
task context and return JSON-serializable summaries for XCom.
"""

import contextlib
import json
import logging
import os
import subprocess
import tempfile
from collections import Counter
from datetime import date
from pathlib import Path
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
from omni_retail.lakehouse.bronze.loader import (
    DbapiTrinoExecutor,
    TrinoConfig,
    load_new,
)
from omni_retail.lakehouse.bronze.specs import TABLES
from omni_retail.serving.clickhouse.cli import main as serving_cli_main
from omni_retail.serving.clickhouse.specs import MARTS

logger = logging.getLogger(__name__)

#: dbt project location inside the Airflow image (compose mounts it read-only).
DBT_PROJECT_DIR = "/opt/airflow/dbt"


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


def run_bronze_load() -> dict[str, object]:
    """Watermark-driven Bronze load of every registered source (Phase 5 §3)."""
    configure_logging()
    storage = BotoObjectStorage(StorageConfig.from_env())
    by_source: dict[str, dict[str, object]] = {}
    loaded_sources = 0
    total_dates = 0
    total_rows = 0
    with contextlib.closing(DbapiTrinoExecutor(TrinoConfig.from_env())) as executor:
        for spec in TABLES.values():
            results = load_new(storage, executor, spec)
            rows = sum(result.row_count for result in results)
            if results:
                loaded_sources += 1
                total_dates += len(results)
                total_rows += rows
            by_source[spec.source_key] = {
                "dates": [result.logical_date.isoformat() for result in results],
                "rows": rows,
            }
    return {
        "sources": len(by_source),
        "loaded_sources": loaded_sources,
        "dates": total_dates,
        "rows": total_rows,
        "by_source": by_source,
    }


class DbtBuildError(RuntimeError):
    """The dbt build subprocess failed (non-zero exit or missing artifacts)."""


def summarize_dbt_results(payload: dict[str, object]) -> dict[str, object]:
    """XCom-friendly summary of a dbt ``run_results.json`` payload."""
    results = cast(list[dict[str, object]], payload.get("results", []))
    statuses = Counter(str(item.get("status")) for item in results if isinstance(item, dict))
    return {
        "elapsed_seconds": round(float(cast(float, payload.get("elapsed_time", 0.0))), 3),
        "result_count": len(results),
        "status_counts": dict(statuses),
    }


def run_dbt_build() -> dict[str, object]:
    """Run a full ``dbt build`` in the worker process (Phase 5 §3).

    The dbt project directory is mounted read-only, so target and log
    artifacts are written to a throwaway per-run directory. The summary comes
    from ``run_results.json``; a non-zero dbt exit fails the task explicitly.
    """
    configure_logging()
    project_dir = Path(os.environ.get("DBT_PROJECT_DIR", DBT_PROJECT_DIR))
    run_dir = Path(tempfile.mkdtemp(prefix="dbt-build-"))
    command = [
        "dbt",
        "build",
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(project_dir),
        "--target-path",
        str(run_dir / "target"),
        "--log-path",
        str(run_dir / "logs"),
    ]
    env = {**os.environ, "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    completed = subprocess.run(command, capture_output=True, text=True, check=False, env=env)
    if completed.returncode != 0:
        tail = "\n".join((completed.stdout + completed.stderr).splitlines()[-30:])
        logger.error("dbt build failed: returncode=%d\n%s", completed.returncode, tail)
        raise DbtBuildError(f"dbt build exited with {completed.returncode}; see task logs")

    results_path = run_dir / "target" / "run_results.json"
    if not results_path.is_file():
        raise DbtBuildError(f"dbt build succeeded but {results_path} is missing")
    summary = summarize_dbt_results(json.loads(results_path.read_text(encoding="utf-8")))
    summary["run_dir"] = str(run_dir)
    logger.info("dbt build completed: %s", summary)
    return summary


def run_serving_rebuild() -> dict[str, object]:
    """Rebuild every ClickHouse serving mart from the current Gold snapshot.

    The serving CLI owns client construction, publication ordering, and
    fail-fast behavior. Keeping this runner thin makes Airflow a coordinator
    rather than a second implementation of the serving boundary.
    """
    configure_logging()
    command = ["rebuild", "--all"]
    exit_code = serving_cli_main(command)
    if exit_code != 0:
        raise RuntimeError(f"ClickHouse serving rebuild failed with exit code {exit_code}")
    summary = {
        "status": "success",
        "command": " ".join(command),
        "mart_count": len(MARTS),
        "marts": list(MARTS),
    }
    logger.info("ClickHouse serving rebuild completed: %s", summary)
    return summary
