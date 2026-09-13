"""Command-line interface for PostgreSQL snapshot extraction."""

import argparse
import logging
import sys
from datetime import date
from typing import cast

import psycopg

from omni_retail.ingestion.common.logging import configure_logging, context_logger
from omni_retail.ingestion.common.storage import (
    BotoObjectStorage,
    StorageConfig,
    StorageError,
)
from omni_retail.ingestion.postgres_snapshot.config import PostgresSourceConfig
from omni_retail.ingestion.postgres_snapshot.extract import SnapshotConnection, snapshot_table
from omni_retail.ingestion.postgres_snapshot.tables import table_by_name
from omni_retail.ingestion.postgres_snapshot.watermark import load_watermark

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m omni_retail.ingestion.postgres_snapshot",
        description="Extract an OLTP table snapshot into the archive bucket (Parquet + manifest).",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser(
        "run", help="extract one table for a logical date (incremental by default)"
    )
    run_parser.add_argument(
        "--table",
        required=True,
        help="categories | customers | products | orders | order_items | payments | shipments",
    )
    run_parser.add_argument(
        "--date",
        type=date.fromisoformat,
        required=True,
        help="logical date (YYYY-MM-DD) addressed by the object key and batch_id",
    )
    run_parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="ignore the watermark and extract a full snapshot (bootstrap)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = build_parser().parse_args(argv)
    try:
        spec = table_by_name(args.table)
        config = PostgresSourceConfig.from_env()
        storage = BotoObjectStorage(StorageConfig.from_env())
        with psycopg.connect(config.conninfo()) as conn:
            watermark = None if args.full_refresh else load_watermark(storage, spec)
            manifest = snapshot_table(
                storage,
                cast(SnapshotConnection, conn),
                spec,
                logical_date=args.date,
                watermark=watermark,
            )
        log = context_logger(__name__, source=spec.source_name, logical_date=args.date.isoformat())
        log.info(
            "run result: batch_id=%s status=%s row_count=%d object_key=%s",
            manifest.batch_id,
            manifest.status,
            manifest.row_count,
            manifest.object_key,
        )
        return 0
    except (ValueError, StorageError, psycopg.Error) as error:
        logger.error("run failed: %s", error)
        return 1


if __name__ == "__main__":
    sys.exit(main())
