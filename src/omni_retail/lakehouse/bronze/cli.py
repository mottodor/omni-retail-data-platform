"""Command-line interface for the Bronze loader (Phase 5 design spec §4)."""

import argparse
import contextlib
import logging
import sys
from datetime import date

from omni_retail.ingestion.common.logging import configure_logging, context_logger
from omni_retail.ingestion.common.storage import (
    BotoObjectStorage,
    ObjectStorage,
    StorageConfig,
    StorageError,
)
from omni_retail.lakehouse.bronze.loader import (
    DbapiTrinoExecutor,
    LoadError,
    TrinoConfig,
    TrinoExecutor,
    load_with_retry,
)
from omni_retail.lakehouse.bronze.readers import BronzeReadError
from omni_retail.lakehouse.bronze.specs import TABLES, BronzeTableSpec, spec_by_source

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m omni_retail.lakehouse.bronze",
        description="Load raw archive objects into Iceberg Bronze tables via Trino.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="load one logical date of one source")
    run_parser.add_argument(
        "--source",
        required=True,
        help="OLTP table name (orders, ...) or API source name (fx-rates, ...)",
    )
    run_parser.add_argument(
        "--date",
        type=date.fromisoformat,
        required=True,
        help="logical date (YYYY-MM-DD) addressed by batch_id and the partition",
    )

    run_all_parser = subparsers.add_parser(
        "run-all", help="load every source for one logical date; fails fast, restartable"
    )
    run_all_parser.add_argument("--date", type=date.fromisoformat, required=True)
    return parser


def run_one(
    spec: BronzeTableSpec,
    storage: ObjectStorage,
    executor: TrinoExecutor,
    logical_date: date,
    catalog: str = "iceberg",
) -> int:
    """Load one source for the logical date; returns a process exit code."""
    load_with_retry(storage, executor, spec, logical_date=logical_date, catalog=catalog)
    return 0


def run_all(
    storage: ObjectStorage, executor: TrinoExecutor, logical_date: date, catalog: str = "iceberg"
) -> int:
    """Load every registered source; empty days are warnings, errors fail fast."""
    log = context_logger(__name__, logical_date=logical_date.isoformat())
    for spec in TABLES.values():
        result = load_with_retry(
            storage, executor, spec, logical_date=logical_date, catalog=catalog
        )
        if result.status == "loaded":
            log.info("run-all progress: source=%s row_count=%d", result.source, result.row_count)
    return 0


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = build_parser().parse_args(argv)
    try:
        # Resolve the spec first so unknown sources fail before any client is built.
        spec = spec_by_source(args.source) if args.command == "run" else None
        storage = BotoObjectStorage(StorageConfig.from_env())
        with contextlib.closing(DbapiTrinoExecutor(TrinoConfig.from_env())) as executor:
            if args.command == "run":
                assert spec is not None
                return run_one(spec, storage, executor, args.date)
            return run_all(storage, executor, args.date)
    except (LoadError, BronzeReadError, StorageError, ValueError) as error:
        logger.error("%s failed: %s", args.command, error)
        return 1


if __name__ == "__main__":
    sys.exit(main())
