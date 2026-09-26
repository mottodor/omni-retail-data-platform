"""Command-line interface for the serving publisher (Phase 6, ADR 0004)."""

import argparse
import contextlib
import logging
import sys

from clickhouse_connect.driver.exceptions import Error as ClickHouseError

from omni_retail.ingestion.common.logging import configure_logging, context_logger
from omni_retail.lakehouse.bronze.loader import DbapiTrinoExecutor, TrinoConfig, TrinoExecutor
from omni_retail.serving.clickhouse.client import ClickHouseConnectClient, ClickHouseExecutor
from omni_retail.serving.clickhouse.config import ClickHouseConfig
from omni_retail.serving.clickhouse.publisher import PublishError, publish, rebuild
from omni_retail.serving.clickhouse.specs import MartSpec, mart_by_name

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m omni_retail.serving.clickhouse",
        description=(
            "Publish Gold marts from Iceberg (via Trino) into ClickHouse "
            "serving tables through the atomic staging swap."
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    publish_parser = subparsers.add_parser(
        "publish", help="swap in a fresh full snapshot of one mart"
    )
    publish_parser.add_argument("--mart", required=True, help="mart name (mart_daily_sales, ...)")

    rebuild_parser = subparsers.add_parser(
        "rebuild", help="recreate the mart DDL if dropped, then publish"
    )
    rebuild_parser.add_argument("--mart", required=True, help="mart name (mart_daily_sales, ...)")
    return parser


def run_command(
    command: str,
    spec: MartSpec,
    trino_executor: TrinoExecutor,
    ch_executor: ClickHouseExecutor,
) -> int:
    """Dispatch one CLI command over injected executors; returns an exit code."""
    if command == "publish":
        publish(trino_executor, ch_executor, spec)
    else:
        rebuild(trino_executor, ch_executor, spec)
    return 0


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = build_parser().parse_args(argv)
    log = context_logger(__name__, command=args.command, mart=args.mart)
    try:
        # Resolve the spec first so unknown marts fail before any client is built.
        spec = mart_by_name(args.mart)
        with (
            contextlib.closing(DbapiTrinoExecutor(TrinoConfig.from_env())) as trino_executor,
            contextlib.closing(ClickHouseConnectClient(ClickHouseConfig.from_env())) as ch_executor,
        ):
            exit_code = run_command(args.command, spec, trino_executor, ch_executor)
        log.info("command completed with exit_code=%d", exit_code)
        return exit_code
    except (PublishError, ClickHouseError, ValueError) as error:
        logger.error("%s failed: %s", args.command, error)
        return 1


if __name__ == "__main__":
    sys.exit(main())
