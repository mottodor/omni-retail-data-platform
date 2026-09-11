"""Command-line interface for the file ingestion pipeline."""

import argparse
import logging
import sys
from datetime import UTC, date, datetime

from omni_retail.ingestion.common.logging import configure_logging, context_logger
from omni_retail.ingestion.common.storage import (
    BotoObjectStorage,
    ObjectStorage,
    StorageConfig,
    StorageError,
)
from omni_retail.ingestion.files.flow import FileFlowError, process_incoming
from omni_retail.ingestion.files.schemas import schema_by_name

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m omni_retail.ingestion.files",
        description=(
            "Process vendor files through the landing -> processing -> archive | rejected flow."
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    process_parser = subparsers.add_parser(
        "process", help="validate and archive pending incoming files of one source"
    )
    process_parser.add_argument(
        "--source",
        required=True,
        help=(
            "source name, e.g. supplier-prices, partner-products, historical-orders, supplier-stock"
        ),
    )
    process_parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        help="logical run date (YYYY-MM-DD) used for archive paths; default: today UTC",
    )
    process_parser.add_argument(
        "--fail-on-rejected",
        action="store_true",
        help="fail the run if any file or row was quarantined",
    )
    process_parser.add_argument(
        "--limit", type=int, default=None, help="process at most N pending files"
    )
    return parser


def run_process(args: argparse.Namespace, storage: ObjectStorage | None = None) -> int:
    """Execute the ``process`` command against an injectable storage."""
    schema = schema_by_name(args.source)
    run_date = args.date or datetime.now(UTC).date()
    effective_storage = storage or BotoObjectStorage(StorageConfig.from_env())

    outcomes = process_incoming(
        effective_storage,
        schema,
        run_date,
        fail_on_rejected=args.fail_on_rejected,
        limit=args.limit,
    )

    log = context_logger(__name__, source=schema.name, run_date=run_date.isoformat())
    for outcome in outcomes:
        log.info(
            "batch result: filename=%s status=%s row_count=%d rejected_row_count=%d",
            outcome.filename,
            outcome.manifest.status,
            outcome.manifest.row_count,
            outcome.manifest.rejected_row_count,
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = build_parser().parse_args(argv)
    try:
        return run_process(args)
    except (ValueError, StorageError, FileFlowError) as error:
        logger.error("process failed: %s", error)
        return 1


if __name__ == "__main__":
    sys.exit(main())
