"""Command-line interface for the API ingestion pipeline."""

import argparse
import contextlib
import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta

from omni_retail.ingestion.api import delivery as delivery_source
from omni_retail.ingestion.api import fx as fx_source
from omni_retail.ingestion.api import marketing as marketing_source
from omni_retail.ingestion.api.client import ApiClient, ApiClientConfig, ApiClientError
from omni_retail.ingestion.api.pipeline import RawPage, persist_api_batch
from omni_retail.ingestion.common.logging import configure_logging, context_logger
from omni_retail.ingestion.common.manifest import BatchManifest
from omni_retail.ingestion.common.storage import (
    BotoObjectStorage,
    ObjectStorage,
    StorageConfig,
    StorageError,
)

logger = logging.getLogger(__name__)

PageFetcher = Callable[[ApiClient, date, int | None], tuple[RawPage, ...]]


@dataclass(frozen=True)
class ApiSourceSpec:
    """One registered API source: identity plus its raw page fetcher."""

    name: str
    schema_version: str
    fetch: PageFetcher


SOURCE_SPECS: tuple[ApiSourceSpec, ...] = (
    ApiSourceSpec(fx_source.SOURCE_NAME, fx_source.SCHEMA_VERSION, fx_source.fetch_pages),
    ApiSourceSpec(
        marketing_source.SOURCE_NAME,
        marketing_source.SCHEMA_VERSION,
        marketing_source.fetch_pages,
    ),
    ApiSourceSpec(
        delivery_source.SOURCE_NAME,
        delivery_source.SCHEMA_VERSION,
        delivery_source.fetch_pages,
    ),
)


def spec_by_name(name: str) -> ApiSourceSpec:
    """Return the API source registered under ``name``."""
    for spec in SOURCE_SPECS:
        if spec.name == name:
            return spec
    known = ", ".join(spec.name for spec in SOURCE_SPECS)
    raise ValueError(f"unknown api source: {name!r} (known: {known})")


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be >= 1")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m omni_retail.ingestion.api",
        description="Fetch raw API pages into the archive bucket (durable raw storage).",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="ingest one logical date of one source")
    run_parser.add_argument(
        "--source", required=True, help="fx-rates | marketing-campaigns | deliveries"
    )
    run_parser.add_argument(
        "--date",
        type=date.fromisoformat,
        required=True,
        help="logical date (YYYY-MM-DD) addressed by batch_id and raw page keys",
    )
    run_parser.add_argument("--page-size", type=_positive_int, default=None)

    backfill_parser = subparsers.add_parser(
        "backfill",
        help="ingest a date range sequentially; fails fast and is restartable",
    )
    backfill_parser.add_argument("--source", required=True)
    backfill_parser.add_argument("--from", dest="date_from", type=date.fromisoformat, required=True)
    backfill_parser.add_argument("--to", dest="date_to", type=date.fromisoformat, required=True)
    backfill_parser.add_argument("--page-size", type=_positive_int, default=None)
    return parser


def run_batch(
    spec: ApiSourceSpec,
    client: ApiClient,
    storage: ObjectStorage,
    logical_date: date,
    page_size: int | None,
) -> BatchManifest:
    """Fetch and persist one logical batch; returns its durable manifest."""
    log = context_logger(__name__, source=spec.name, logical_date=logical_date.isoformat())
    pages = spec.fetch(client, logical_date, page_size)
    manifest = persist_api_batch(
        storage,
        source=spec.name,
        schema_version=spec.schema_version,
        logical_date=logical_date,
        pages=pages,
    )
    log.info(
        "api batch result: batch_id=%s status=%s pages=%d row_count=%d object_key=%s",
        manifest.batch_id,
        manifest.status,
        len(pages),
        manifest.row_count,
        manifest.object_key,
    )
    return manifest


def run_one(
    spec: ApiSourceSpec, client: ApiClient, storage: ObjectStorage, args: argparse.Namespace
) -> int:
    run_batch(spec, client, storage, args.date, args.page_size)
    return 0


def run_backfill(
    spec: ApiSourceSpec, client: ApiClient, storage: ObjectStorage, args: argparse.Namespace
) -> int:
    if args.date_from > args.date_to:
        raise ValueError(f"--from ({args.date_from}) must not be after --to ({args.date_to})")
    current = args.date_from
    while current <= args.date_to:
        run_batch(spec, client, storage, current, args.page_size)
        current += timedelta(days=1)
    return 0


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = build_parser().parse_args(argv)
    try:
        spec = spec_by_name(args.source)
        client = ApiClient(ApiClientConfig.from_env())
        with contextlib.closing(client):
            storage = BotoObjectStorage(StorageConfig.from_env())
            if args.command == "run":
                return run_one(spec, client, storage, args)
            return run_backfill(spec, client, storage, args)
    except (ValueError, StorageError, ApiClientError) as error:
        logger.error("%s failed: %s", args.command, error)
        return 1


if __name__ == "__main__":
    sys.exit(main())
