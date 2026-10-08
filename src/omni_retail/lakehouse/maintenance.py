"""CLI for bounded Iceberg batch Bronze snapshot maintenance."""

from __future__ import annotations

import argparse
import contextlib
import logging
import sys
from collections.abc import Callable

import trino

from omni_retail.ingestion.common.logging import configure_logging, context_logger
from omni_retail.lakehouse.bronze.loader import bronze_schema_from_env
from omni_retail.lakehouse.bronze.specs import TABLES
from omni_retail.lakehouse.trino import DbapiTrinoExecutor, TrinoConfig, TrinoExecutor

from .cdc_maintenance import (  # pyright: ignore[reportMissingImports]
    CdcMaintenancePolicy,
    compact_cdc_data_files,
    expire_cdc_snapshots,
    plan_cdc_maintenance,
)
from .snapshot_maintenance import (
    SnapshotExpirationPolicy,
    SnapshotExpirationResult,
    SnapshotMaintenanceError,
    expire_table_snapshots,
    plan_table_snapshots,
)

logger = logging.getLogger(__name__)
MaintenanceOperation = Callable[..., SnapshotExpirationResult]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m omni_retail.lakehouse.maintenance",
        description="Plan or apply bounded snapshot expiration to batch Bronze tables.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan_parser = subparsers.add_parser("plan", help="read-only expiration preview")
    _add_table_argument(plan_parser)

    expire_parser = subparsers.add_parser(
        "expire", help="expire eligible snapshots after explicit confirmation"
    )
    _add_table_argument(expire_parser)
    expire_parser.add_argument(
        "--confirm",
        action="store_true",
        help="required acknowledgement that snapshot expiration is irreversible",
    )

    subparsers.add_parser(
        "cdc-plan",
        help="read-only CDC data-file compaction and snapshot-expiration preview",
    )
    cdc_apply_parser = subparsers.add_parser(
        "cdc-apply",
        help="compact the CDC ledger and expire eligible snapshots",
    )
    cdc_apply_parser.add_argument(
        "--confirm",
        action="store_true",
        help="required acknowledgement that snapshot expiration is irreversible",
    )
    return parser


def _add_table_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--table",
        choices=tuple(TABLES),
        help="one registered batch Bronze table; omit to process all registered tables",
    )


def run_tables(
    executor: TrinoExecutor,
    table_names: tuple[str, ...],
    policy: SnapshotExpirationPolicy,
    operation: MaintenanceOperation,
    *,
    catalog: str,
    schema: str,
) -> tuple[SnapshotExpirationResult, ...]:
    """Run independent tables, aggregate failures, and retain successful results."""
    results: list[SnapshotExpirationResult] = []
    failures: list[str] = []
    for table_name in table_names:
        log = context_logger(__name__, table=table_name, operation=operation.__name__)
        try:
            result = operation(
                executor,
                table_name,
                policy,
                catalog=catalog,
                schema=schema,
            )
        except Exception as error:  # independent table failures are aggregated and re-raised
            message = getattr(error, "message", str(error))
            failures.append(f"{table_name}: {type(error).__name__}: {message}")
            log.error("snapshot maintenance failed: %s", message)
            continue
        results.append(result)
        log.info(
            "snapshot maintenance complete: status=%s snapshots_before=%d "
            "snapshots_after=%d age_eligible=%d removed_snapshots=%d "
            "removed_manifests=%d removed_data_files=%d concurrent_main_change=%s",
            result.status,
            result.snapshots_before,
            result.snapshots_after,
            len(result.age_eligible_snapshot_ids),
            len(result.removed_snapshot_ids),
            result.removed_manifest_count,
            result.removed_data_file_count,
            result.concurrent_main_change,
        )
        if result.removed_manifest_sample:
            log.info("removed manifest sample: %s", result.removed_manifest_sample)
        if result.removed_data_file_sample:
            log.info("removed data-file sample: %s", result.removed_data_file_sample)
    if failures:
        raise SnapshotMaintenanceError(
            f"snapshot maintenance failed for {len(failures)} table(s): " + "; ".join(failures)
        )
    return tuple(results)


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = build_parser().parse_args(argv)
    if args.command in {"expire", "cdc-apply"} and not args.confirm:
        logger.error("%s requires --confirm; run its plan first", args.command)
        return 2

    try:
        config = TrinoConfig.from_env()
        schema = bronze_schema_from_env()
        if args.command in {"cdc-plan", "cdc-apply"}:
            cdc_policy = CdcMaintenancePolicy.from_env()
            with contextlib.closing(DbapiTrinoExecutor(config)) as executor:
                if args.command == "cdc-plan":
                    plan = plan_cdc_maintenance(
                        executor,
                        cdc_policy,
                        catalog=config.catalog,
                        schema=schema,
                    )
                    logger.info("CDC maintenance plan: %s", plan.as_dict())
                else:
                    logger.warning(
                        "CDC snapshot expiration is irreversible beyond the retained window"
                    )
                    compaction = compact_cdc_data_files(
                        executor,
                        cdc_policy,
                        catalog=config.catalog,
                        schema=schema,
                    )
                    logger.info("CDC maintenance stage complete: %s", compaction.as_dict())
                    expiration = expire_cdc_snapshots(
                        executor,
                        cdc_policy,
                        catalog=config.catalog,
                        schema=schema,
                    )
                    logger.info("CDC maintenance stage complete: %s", expiration.as_dict())
            return 0

        policy = SnapshotExpirationPolicy.from_env()
        table_names = (args.table,) if args.table else tuple(TABLES)
        operation = expire_table_snapshots if args.command == "expire" else plan_table_snapshots
        with contextlib.closing(DbapiTrinoExecutor(config)) as executor:
            results = run_tables(
                executor,
                table_names,
                policy,
                operation,
                catalog=config.catalog,
                schema=schema,
            )
        logger.info(
            "snapshot maintenance run complete: command=%s tables=%d statuses=%s",
            args.command,
            len(results),
            {
                status: sum(result.status == status for result in results)
                for status in {result.status for result in results}
            },
        )
        return 0
    except (SnapshotMaintenanceError, ValueError, trino.exceptions.Error) as error:
        logger.error("snapshot maintenance stopped: %s", error)
        return 1


if __name__ == "__main__":
    sys.exit(main())
