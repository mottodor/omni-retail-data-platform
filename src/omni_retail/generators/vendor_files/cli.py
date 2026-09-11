"""Command-line interface for the vendor file generator."""

import argparse
import logging
import sys
from datetime import UTC, date, datetime
from pathlib import Path

from omni_retail.generators.vendor_files.generator import generate_payload, upload_payload
from omni_retail.ingestion.common.logging import configure_logging, context_logger
from omni_retail.ingestion.common.storage import (
    BotoObjectStorage,
    ObjectStorage,
    StorageConfig,
    StorageError,
)
from omni_retail.ingestion.files.schemas import schema_by_name

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m omni_retail.generators.vendor_files",
        description="Generate deterministic vendor files (supplier CSV, partner JSON).",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate_parser = subparsers.add_parser("generate", help="generate a vendor file payload")
    generate_parser.add_argument(
        "--source", required=True, help="supplier-prices or partner-products"
    )
    generate_parser.add_argument("--rows", type=int, default=200)
    generate_parser.add_argument("--seed", type=int, default=7)
    generate_parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        help="logical run date (YYYY-MM-DD) encoded in the filename; default: today UTC",
    )
    generate_parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="write the generated file into this local directory",
    )
    generate_parser.add_argument(
        "--upload",
        action="store_true",
        help="upload the generated file into the source landing drop zone",
    )
    return parser


def run_generate(args: argparse.Namespace, storage: ObjectStorage | None = None) -> int:
    """Execute the ``generate`` command; requires --upload and/or --output-dir."""
    schema = schema_by_name(args.source)
    run_date = args.date or datetime.now(UTC).date()
    if not args.upload and args.output_dir is None:
        raise ValueError("specify --upload and/or --output-dir for the generated file")

    payload = generate_payload(schema, seed=args.seed, rows=args.rows, run_date=run_date)
    log = context_logger(
        __name__,
        source=schema.name,
        rows=args.rows,
        seed=args.seed,
        run_date=run_date.isoformat(),
    )

    if args.output_dir is not None:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        target = (
            args.output_dir
            / f"{schema.name}_{run_date:%Y%m%d}_s{args.seed}_n{args.rows}.{schema.format}"
        )
        target.write_bytes(payload)
        log.info("file written: path=%s size_bytes=%d", target, len(payload))

    if args.upload:
        effective_storage = storage or BotoObjectStorage(StorageConfig.from_env())
        key = upload_payload(
            effective_storage,
            schema,
            payload,
            seed=args.seed,
            rows=args.rows,
            run_date=run_date,
        )
        log.info("file uploaded: key=%s size_bytes=%d", key, len(payload))
    return 0


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = build_parser().parse_args(argv)
    try:
        return run_generate(args)
    except (ValueError, StorageError) as error:
        logger.error("generate failed: %s", error)
        return 1


if __name__ == "__main__":
    sys.exit(main())
