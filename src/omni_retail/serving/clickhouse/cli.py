"""Command-line interface for the serving publisher (Phase 6, ADR 0004)."""

import argparse
import contextlib
import logging
import sys
from collections.abc import Sequence
from pathlib import Path

from clickhouse_connect.driver.exceptions import Error as ClickHouseError

from omni_retail.ingestion.common.logging import configure_logging, context_logger
from omni_retail.lakehouse.bronze.loader import DbapiTrinoExecutor, TrinoConfig, TrinoExecutor
from omni_retail.serving.clickhouse.benchmark import run_benchmark, write_report
from omni_retail.serving.clickhouse.client import ClickHouseConnectClient, ClickHouseExecutor
from omni_retail.serving.clickhouse.config import ClickHouseConfig
from omni_retail.serving.clickhouse.publisher import PublishError, publish, rebuild
from omni_retail.serving.clickhouse.specs import MARTS, MartSpec, mart_by_name

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
    rebuild_targets = rebuild_parser.add_mutually_exclusive_group(required=True)
    rebuild_targets.add_argument("--mart", help="mart name (mart_daily_sales, ...)")
    rebuild_targets.add_argument(
        "--all",
        action="store_true",
        help="rebuild every registered mart (registry order; fails fast)",
    )

    benchmark_parser = subparsers.add_parser(
        "benchmark", help="compare one representative Gold query with ClickHouse"
    )
    benchmark_parser.add_argument(
        "--output",
        type=Path,
        default=Path("docs/benchmarks/phase6-trino-vs-clickhouse.md"),
        help="Markdown report path",
    )
    benchmark_parser.add_argument(
        "--repetitions", type=int, default=5, help="runs per engine (minimum 2)"
    )
    return parser


def run_command(
    command: str,
    specs: Sequence[MartSpec],
    trino_executor: TrinoExecutor,
    ch_executor: ClickHouseExecutor,
) -> int:
    """Dispatch one CLI command over injected executors; returns an exit code.

    Fails fast: the first mart error aborts the run (the serving tables
    already processed stay swapped-in and consistent — rerun is safe).
    """
    for spec in specs:
        if command == "publish":
            publish(trino_executor, ch_executor, spec)
        else:
            rebuild(trino_executor, ch_executor, spec)
    return 0


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = build_parser().parse_args(argv)
    mart_label = getattr(args, "mart", None) or "all"
    log = context_logger(__name__, command=args.command, mart=mart_label)
    try:
        specs: list[MartSpec] = []
        if args.command != "benchmark":
            # Resolve the specs first so unknown marts fail before any client
            # is built; `rebuild --all` covers the whole registry in order.
            mart = getattr(args, "mart", None)
            specs = [mart_by_name(mart)] if mart else list(MARTS.values())
        with (
            contextlib.closing(DbapiTrinoExecutor(TrinoConfig.from_env())) as trino_executor,
            contextlib.closing(ClickHouseConnectClient(ClickHouseConfig.from_env())) as ch_executor,
        ):
            if args.command == "benchmark":
                results = run_benchmark(
                    trino_executor,
                    ch_executor,
                    repetitions=args.repetitions,
                )
                write_report(args.output, results, repetitions=args.repetitions)
                log.info("benchmark report written: %s", args.output)
                return 0

            exit_code = run_command(args.command, specs, trino_executor, ch_executor)
        log.info("command completed with exit_code=%d", exit_code)
        return exit_code
    except (PublishError, ClickHouseError, ValueError) as error:
        logger.error("%s failed: %s", args.command, error)
        return 1


if __name__ == "__main__":
    sys.exit(main())
