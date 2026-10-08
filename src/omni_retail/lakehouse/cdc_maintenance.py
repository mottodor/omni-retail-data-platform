"""Bounded Iceberg maintenance for the append-only PostgreSQL CDC ledger."""

from __future__ import annotations

import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Literal

from omni_retail.lakehouse.snapshot_maintenance import (
    PATH_SAMPLE_LIMIT,
    SnapshotExpirationPolicy,
    SnapshotExpirationResult,
    SnapshotMaintenanceError,
    expire_table_snapshots,
    plan_table_snapshots,
)
from omni_retail.lakehouse.trino import TrinoExecutor, validate_schema_name
from omni_retail.streaming.cdc.sql import CDC_COLUMNS, CDC_TABLE

DEFAULT_FILE_SIZE_THRESHOLD_MB = 128
MIN_FILE_SIZE_THRESHOLD_MB = 1
MAX_FILE_SIZE_THRESHOLD_MB = 512
DEFAULT_CDC_RETENTION_DAYS = 30
DEFAULT_CDC_RETAIN_LAST = 10
CDC_TABLES = (CDC_TABLE,)

PlanStatus = Literal["skipped_missing", "no_op", "planned"]
CompactionStatus = Literal["skipped_missing", "no_op", "compacted"]


class CdcMaintenanceError(SnapshotMaintenanceError):
    """CDC metadata, procedure output, or a semantic postcondition is unsafe."""


@dataclass(frozen=True)
class CdcMaintenancePolicy:
    """Validated non-secret compaction and snapshot-retention settings."""

    file_size_threshold_mb: int = DEFAULT_FILE_SIZE_THRESHOLD_MB
    snapshot_retention_days: int = DEFAULT_CDC_RETENTION_DAYS
    snapshot_retain_last: int = DEFAULT_CDC_RETAIN_LAST

    def __post_init__(self) -> None:
        threshold = self.file_size_threshold_mb
        if (
            type(threshold) is not int
            or threshold < MIN_FILE_SIZE_THRESHOLD_MB
            or threshold > MAX_FILE_SIZE_THRESHOLD_MB
        ):
            raise ValueError(
                "CDC file-size threshold must be an integer between "
                f"{MIN_FILE_SIZE_THRESHOLD_MB} and {MAX_FILE_SIZE_THRESHOLD_MB} MB"
            )
        # Reuse the repository and catalog-compatible retention floors.
        _ = self.snapshot_policy

    @property
    def file_size_threshold_bytes(self) -> int:
        return self.file_size_threshold_mb * 1024 * 1024

    @property
    def snapshot_policy(self) -> SnapshotExpirationPolicy:
        return SnapshotExpirationPolicy(
            retention_days=self.snapshot_retention_days,
            retain_last=self.snapshot_retain_last,
        )

    @classmethod
    def from_env(cls) -> CdcMaintenancePolicy:
        return cls(
            file_size_threshold_mb=_environment_integer(
                "ICEBERG_CDC_FILE_SIZE_THRESHOLD_MB", DEFAULT_FILE_SIZE_THRESHOLD_MB
            ),
            snapshot_retention_days=_environment_integer(
                "ICEBERG_CDC_SNAPSHOT_RETENTION_DAYS", DEFAULT_CDC_RETENTION_DAYS
            ),
            snapshot_retain_last=_environment_integer(
                "ICEBERG_CDC_SNAPSHOT_RETAIN_LAST", DEFAULT_CDC_RETAIN_LAST
            ),
        )


@dataclass(frozen=True)
class CandidatePartition:
    event_date: date
    files: int
    small_files: int
    rows: int
    bytes: int

    def as_dict(self) -> dict[str, object]:
        return {
            "event_date": self.event_date.isoformat(),
            "files": self.files,
            "small_files": self.small_files,
            "rows": self.rows,
            "bytes": self.bytes,
        }


@dataclass(frozen=True)
class CdcFileInventory:
    files: int
    rows: int
    bytes: int
    minimum_file_bytes: int
    maximum_file_bytes: int
    candidate_partition_count: int
    candidate_files: int
    candidate_partitions: tuple[CandidatePartition, ...]
    current_file_sample: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "files": self.files,
            "rows": self.rows,
            "bytes": self.bytes,
            "minimum_file_bytes": self.minimum_file_bytes,
            "maximum_file_bytes": self.maximum_file_bytes,
            "candidate_partition_count": self.candidate_partition_count,
            "candidate_files": self.candidate_files,
            "candidate_partitions": [item.as_dict() for item in self.candidate_partitions],
            "current_file_sample": list(self.current_file_sample),
        }


@dataclass(frozen=True)
class RowPreservationResult:
    snapshot_id: int
    snapshot_rows: int
    current_rows: int
    distinct_event_ids: int
    missing_or_changed_rows: int

    def as_dict(self) -> dict[str, object]:
        return {
            "snapshot_id": self.snapshot_id,
            "snapshot_rows": self.snapshot_rows,
            "current_rows": self.current_rows,
            "distinct_event_ids": self.distinct_event_ids,
            "missing_or_changed_rows": self.missing_or_changed_rows,
            "rows_preserved": self.missing_or_changed_rows == 0,
            "event_ids_unique": self.current_rows == self.distinct_event_ids,
        }


@dataclass(frozen=True)
class OptimizeMetrics:
    rewritten_data_files_count: int = 0
    removed_delete_files_count: int = 0
    added_data_files_count: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "rewritten_data_files_count": self.rewritten_data_files_count,
            "removed_delete_files_count": self.removed_delete_files_count,
            "added_data_files_count": self.added_data_files_count,
        }


@dataclass(frozen=True)
class CdcMaintenancePlan:
    table_name: str
    status: PlanStatus
    policy: CdcMaintenancePolicy
    files: CdcFileInventory
    snapshot_plan: SnapshotExpirationResult | None
    current_rows: int
    distinct_event_ids: int

    def as_dict(self) -> dict[str, object]:
        return {
            "stage": "plan",
            "table_name": self.table_name,
            "status": self.status,
            "file_size_threshold_mb": self.policy.file_size_threshold_mb,
            "snapshot_retention_days": self.policy.snapshot_retention_days,
            "snapshot_retain_last": self.policy.snapshot_retain_last,
            "files": self.files.as_dict(),
            "snapshots": (
                _bounded_snapshot_dict(self.snapshot_plan) if self.snapshot_plan else None
            ),
            "current_rows": self.current_rows,
            "distinct_event_ids": self.distinct_event_ids,
            "event_ids_unique": self.current_rows == self.distinct_event_ids,
        }


@dataclass(frozen=True)
class CdcCompactionResult:
    table_name: str
    status: CompactionStatus
    policy: CdcMaintenancePolicy
    before: CdcFileInventory
    after: CdcFileInventory
    metrics: OptimizeMetrics
    preservation: RowPreservationResult | None
    main_snapshot_before: int | None
    main_snapshot_after: int | None
    concurrent_main_change: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "stage": "compact_data_files",
            "table_name": self.table_name,
            "status": self.status,
            "file_size_threshold_mb": self.policy.file_size_threshold_mb,
            "before": self.before.as_dict(),
            "after": self.after.as_dict(),
            "metrics": self.metrics.as_dict(),
            "preservation": self.preservation.as_dict() if self.preservation else None,
            "main_snapshot_before": self.main_snapshot_before,
            "main_snapshot_after": self.main_snapshot_after,
            "concurrent_main_change": self.concurrent_main_change,
        }


@dataclass(frozen=True)
class CdcExpirationResult:
    table_name: str
    snapshot_result: SnapshotExpirationResult
    preservation: RowPreservationResult | None

    def as_dict(self) -> dict[str, object]:
        return {
            "stage": "expire_snapshots",
            "table_name": self.table_name,
            "status": self.snapshot_result.status,
            "snapshot_result": _bounded_snapshot_dict(self.snapshot_result),
            "preservation": self.preservation.as_dict() if self.preservation else None,
        }


def plan_cdc_maintenance(
    executor: TrinoExecutor,
    policy: CdcMaintenancePolicy,
    *,
    catalog: str = "iceberg",
    schema: str = "bronze",
) -> CdcMaintenancePlan:
    """Return a read-only file/snapshot plan for the one CDC ledger."""
    _validate_target(CDC_TABLE, catalog, schema)
    if not _table_exists(executor, catalog, schema):
        return CdcMaintenancePlan(
            table_name=CDC_TABLE,
            status="skipped_missing",
            policy=policy,
            files=_empty_file_inventory(),
            snapshot_plan=None,
            current_rows=0,
            distinct_event_ids=0,
        )

    files = _read_file_inventory(executor, policy, catalog=catalog, schema=schema)
    current_rows, distinct_ids = _read_current_identity(executor, catalog=catalog, schema=schema)
    _require_unique_event_ids(current_rows, distinct_ids)
    snapshot_plan = _plan_snapshots_if_present(executor, policy, catalog=catalog, schema=schema)
    status: PlanStatus = (
        "planned"
        if files.candidate_partition_count
        or (snapshot_plan is not None and snapshot_plan.status == "planned")
        else "no_op"
    )
    return CdcMaintenancePlan(
        table_name=CDC_TABLE,
        status=status,
        policy=policy,
        files=files,
        snapshot_plan=snapshot_plan,
        current_rows=current_rows,
        distinct_event_ids=distinct_ids,
    )


def compact_cdc_data_files(
    executor: TrinoExecutor,
    policy: CdcMaintenancePolicy,
    *,
    catalog: str = "iceberg",
    schema: str = "bronze",
) -> CdcCompactionResult:
    """Compact current CDC files and prove all pre-existing raw rows survive."""
    _validate_target(CDC_TABLE, catalog, schema)
    if not _table_exists(executor, catalog, schema):
        empty = _empty_file_inventory()
        return CdcCompactionResult(
            CDC_TABLE,
            "skipped_missing",
            policy,
            empty,
            empty,
            OptimizeMetrics(),
            None,
            None,
            None,
            False,
        )

    before = _read_file_inventory(executor, policy, catalog=catalog, schema=schema)
    current_rows, distinct_ids = _read_current_identity(executor, catalog=catalog, schema=schema)
    _require_unique_event_ids(current_rows, distinct_ids)
    if before.files == 0:
        return CdcCompactionResult(
            CDC_TABLE,
            "no_op",
            policy,
            before,
            before,
            OptimizeMetrics(),
            None,
            None,
            None,
            False,
        )

    main_before = _main_snapshot(executor, catalog=catalog, schema=schema)[0]
    if before.candidate_partition_count == 0:
        preservation = _validate_preservation(executor, main_before, catalog=catalog, schema=schema)
        return CdcCompactionResult(
            CDC_TABLE,
            "no_op",
            policy,
            before,
            before,
            OptimizeMetrics(),
            preservation,
            main_before,
            main_before,
            False,
        )

    # ALTER TABLE EXECUTE returns one metric-name/value row per procedure metric.
    rows = executor.fetch(_optimize_sql(policy, catalog=catalog, schema=schema))
    metrics = _parse_optimize_metrics(rows)
    after = _read_file_inventory(executor, policy, catalog=catalog, schema=schema)
    main_after, parent_after = _main_snapshot(executor, catalog=catalog, schema=schema)
    if metrics.rewritten_data_files_count > 0 and main_after == main_before:
        raise CdcMaintenanceError(
            "optimize reported rewritten files but the CDC main snapshot did not advance"
        )
    preservation = _validate_preservation(executor, main_before, catalog=catalog, schema=schema)
    concurrent_main_change = main_after != main_before and (
        metrics.rewritten_data_files_count == 0 or parent_after != main_before
    )
    return CdcCompactionResult(
        table_name=CDC_TABLE,
        status="compacted" if metrics.rewritten_data_files_count else "no_op",
        policy=policy,
        before=before,
        after=after,
        metrics=metrics,
        preservation=preservation,
        main_snapshot_before=main_before,
        main_snapshot_after=main_after,
        concurrent_main_change=concurrent_main_change,
    )


def expire_cdc_snapshots(
    executor: TrinoExecutor,
    policy: CdcMaintenancePolicy,
    *,
    catalog: str = "iceberg",
    schema: str = "bronze",
) -> CdcExpirationResult:
    """Expire CDC history through shared ref protection and verify current rows."""
    _validate_target(CDC_TABLE, catalog, schema)
    if not _table_exists(executor, catalog, schema):
        return CdcExpirationResult(
            CDC_TABLE,
            _empty_snapshot_result("skipped_missing", policy),
            None,
        )
    if _snapshot_count(executor, catalog=catalog, schema=schema) == 0:
        return CdcExpirationResult(
            CDC_TABLE,
            _empty_snapshot_result("no_op", policy),
            None,
        )

    main_before = _main_snapshot(executor, catalog=catalog, schema=schema)[0]
    current_rows, distinct_ids = _read_current_identity(executor, catalog=catalog, schema=schema)
    _require_unique_event_ids(current_rows, distinct_ids)
    result = expire_table_snapshots(
        executor,
        CDC_TABLE,
        policy.snapshot_policy,
        catalog=catalog,
        schema=schema,
        allowed_tables=CDC_TABLES,
    )
    preservation = _validate_preservation(executor, main_before, catalog=catalog, schema=schema)
    return CdcExpirationResult(CDC_TABLE, result, preservation)


def _environment_integer(name: str, default: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        return int(raw)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from error


def _validate_target(table_name: str, catalog: str, schema: str) -> None:
    validate_schema_name(catalog)
    validate_schema_name(schema)
    if table_name != CDC_TABLE:
        raise ValueError(f"CDC maintenance only allows table {CDC_TABLE!r}")


def _identifier(value: str) -> str:
    return f'"{value}"'


def _qualified_table(catalog: str, schema: str) -> str:
    return ".".join(_identifier(value) for value in (catalog, schema, CDC_TABLE))


def _metadata_table(suffix: str, catalog: str, schema: str) -> str:
    return ".".join(
        (_identifier(catalog), _identifier(schema), _identifier(f"{CDC_TABLE}${suffix}"))
    )


def _table_exists(executor: TrinoExecutor, catalog: str, schema: str) -> bool:
    rows = executor.fetch(
        f"SELECT count(*) FROM {_identifier(catalog)}.information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_name = '{CDC_TABLE}'"
    )
    count = _single_non_negative_integer(rows, "table-existence")
    if count not in (0, 1):
        raise CdcMaintenanceError(f"malformed table-existence count: {count}")
    return count == 1


def _snapshot_count(executor: TrinoExecutor, *, catalog: str, schema: str) -> int:
    return _single_non_negative_integer(
        executor.fetch(f"SELECT count(*) FROM {_metadata_table('snapshots', catalog, schema)}"),
        "snapshot-count",
    )


def _plan_snapshots_if_present(
    executor: TrinoExecutor,
    policy: CdcMaintenancePolicy,
    *,
    catalog: str,
    schema: str,
) -> SnapshotExpirationResult | None:
    if _snapshot_count(executor, catalog=catalog, schema=schema) == 0:
        return None
    return plan_table_snapshots(
        executor,
        CDC_TABLE,
        policy.snapshot_policy,
        catalog=catalog,
        schema=schema,
        allowed_tables=CDC_TABLES,
    )


def _read_file_inventory(
    executor: TrinoExecutor,
    policy: CdcMaintenancePolicy,
    *,
    catalog: str,
    schema: str,
) -> CdcFileInventory:
    rows = executor.fetch(
        'SELECT "partition"."event_date", file_path, record_count, file_size_in_bytes '
        f"FROM {_metadata_table('files', catalog, schema)} ORDER BY 1, 2"
    )
    parsed: list[tuple[date, str, int, int]] = []
    by_partition: dict[date, list[tuple[int, int]]] = defaultdict(list)
    for row in rows:
        if len(row) != 4:
            raise CdcMaintenanceError("malformed CDC data-file metadata row")
        event_date, path, record_count, file_size = row
        if (
            type(event_date) is not date
            or not isinstance(path, str)
            or not path
            or type(record_count) is not int
            or record_count < 0
            or type(file_size) is not int
            or file_size < 0
        ):
            raise CdcMaintenanceError("invalid CDC data-file metadata attributes")
        parsed.append((event_date, path, record_count, file_size))
        by_partition[event_date].append((record_count, file_size))

    candidates: list[CandidatePartition] = []
    threshold = policy.file_size_threshold_bytes
    for event_date, partition_files in sorted(by_partition.items()):
        small_files = sum(file_size < threshold for _, file_size in partition_files)
        if small_files >= 2:
            candidates.append(
                CandidatePartition(
                    event_date=event_date,
                    files=len(partition_files),
                    small_files=small_files,
                    rows=sum(record_count for record_count, _ in partition_files),
                    bytes=sum(file_size for _, file_size in partition_files),
                )
            )
    sizes = [file_size for _, _, _, file_size in parsed]
    return CdcFileInventory(
        files=len(parsed),
        rows=sum(record_count for _, _, record_count, _ in parsed),
        bytes=sum(sizes),
        minimum_file_bytes=min(sizes, default=0),
        maximum_file_bytes=max(sizes, default=0),
        candidate_partition_count=len(candidates),
        candidate_files=sum(partition.small_files for partition in candidates),
        candidate_partitions=tuple(candidates[:PATH_SAMPLE_LIMIT]),
        current_file_sample=tuple(sorted(path for _, path, _, _ in parsed)[:PATH_SAMPLE_LIMIT]),
    )


def _read_current_identity(
    executor: TrinoExecutor, *, catalog: str, schema: str
) -> tuple[int, int]:
    rows = executor.fetch(
        f"SELECT count(*), count(DISTINCT event_id) FROM {_qualified_table(catalog, schema)}"
    )
    if (
        len(rows) != 1
        or len(rows[0]) != 2
        or type(rows[0][0]) is not int
        or type(rows[0][1]) is not int
        or rows[0][0] < 0
        or rows[0][1] < 0
    ):
        raise CdcMaintenanceError("malformed CDC row-identity result")
    return rows[0][0], rows[0][1]


def _require_unique_event_ids(rows: int, distinct_event_ids: int) -> None:
    if rows != distinct_event_ids:
        raise CdcMaintenanceError(
            f"CDC event_id uniqueness failed: rows={rows} distinct_event_ids={distinct_event_ids}"
        )


def _main_snapshot(executor: TrinoExecutor, *, catalog: str, schema: str) -> tuple[int, int | None]:
    rows = executor.fetch(
        "SELECT s.snapshot_id, s.parent_id "
        f"FROM {_metadata_table('snapshots', catalog, schema)} s "
        f"JOIN {_metadata_table('refs', catalog, schema)} r "
        "ON r.snapshot_id = s.snapshot_id WHERE r.name = 'main'"
    )
    if len(rows) != 1 or len(rows[0]) != 2:
        raise CdcMaintenanceError("expected exactly one CDC main snapshot")
    snapshot_id, parent_id = rows[0]
    if type(snapshot_id) is not int or (parent_id is not None and type(parent_id) is not int):
        raise CdcMaintenanceError("malformed CDC main snapshot identifiers")
    return snapshot_id, parent_id


def _validate_preservation(
    executor: TrinoExecutor,
    snapshot_id: int,
    *,
    catalog: str,
    schema: str,
) -> RowPreservationResult:
    table = _qualified_table(catalog, schema)
    snapshot_rows = _single_non_negative_integer(
        executor.fetch(f"SELECT count(*) FROM {table} FOR VERSION AS OF {snapshot_id}"),
        "snapshot-row-count",
    )
    current_rows, distinct_ids = _read_current_identity(executor, catalog=catalog, schema=schema)
    columns = ", ".join(_identifier(name) for name, _ in CDC_COLUMNS)
    missing_or_changed = _single_non_negative_integer(
        executor.fetch(
            "SELECT count(*) FROM ("
            f"SELECT {columns} FROM {table} FOR VERSION AS OF {snapshot_id} EXCEPT "
            f"SELECT {columns} FROM {table}"
            ") AS missing_or_changed"
        ),
        "missing-or-changed-row-count",
    )
    if missing_or_changed:
        raise CdcMaintenanceError(
            f"CDC raw-row preservation failed for snapshot {snapshot_id}: "
            f"missing_or_changed_rows={missing_or_changed}"
        )
    _require_unique_event_ids(current_rows, distinct_ids)
    if current_rows < snapshot_rows:
        raise CdcMaintenanceError(
            f"CDC current row count regressed: snapshot={snapshot_rows} current={current_rows}"
        )
    return RowPreservationResult(
        snapshot_id=snapshot_id,
        snapshot_rows=snapshot_rows,
        current_rows=current_rows,
        distinct_event_ids=distinct_ids,
        missing_or_changed_rows=missing_or_changed,
    )


def _optimize_sql(policy: CdcMaintenancePolicy, *, catalog: str, schema: str) -> str:
    return (
        f"ALTER TABLE {_qualified_table(catalog, schema)} EXECUTE optimize("
        f"file_size_threshold => '{policy.file_size_threshold_mb}MB')"
    )


def _parse_optimize_metrics(rows: list[tuple[object, ...]]) -> OptimizeMetrics:
    expected = {
        "rewritten_data_files_count",
        "removed_delete_files_count",
        "added_data_files_count",
    }
    metrics: dict[str, int] = {}
    for row in rows:
        if len(row) != 2 or not isinstance(row[0], str):
            raise CdcMaintenanceError("malformed optimize procedure metric row")
        name = row[0]
        if name not in expected or name in metrics:
            raise CdcMaintenanceError(f"unexpected or duplicate optimize metric {name!r}")
        raw_value = row[1]
        if type(raw_value) is int:
            value = raw_value
        elif isinstance(raw_value, str) and raw_value.isdigit():
            try:
                value = int(raw_value)
            except ValueError as error:  # defensive despite the digit guard
                raise CdcMaintenanceError(
                    f"invalid optimize metric value for {name}: {raw_value!r}"
                ) from error
        else:
            raise CdcMaintenanceError(f"invalid optimize metric value for {name}: {raw_value!r}")
        if value < 0:
            raise CdcMaintenanceError(f"negative optimize metric value for {name}")
        metrics[name] = value
    if set(metrics) != expected:
        raise CdcMaintenanceError(
            f"optimize metrics must match exactly: expected={sorted(expected)} "
            f"actual={sorted(metrics)}"
        )
    rewritten = metrics["rewritten_data_files_count"]
    added = metrics["added_data_files_count"]
    if (rewritten == 0) != (added == 0):
        raise CdcMaintenanceError(
            f"optimize rewritten/added file metrics disagree: rewritten={rewritten} added={added}"
        )
    return OptimizeMetrics(
        rewritten_data_files_count=rewritten,
        removed_delete_files_count=metrics["removed_delete_files_count"],
        added_data_files_count=added,
    )


def _single_non_negative_integer(rows: list[tuple[object, ...]], label: str) -> int:
    if len(rows) != 1 or len(rows[0]) != 1 or type(rows[0][0]) is not int or rows[0][0] < 0:
        raise CdcMaintenanceError(f"malformed {label} result")
    return rows[0][0]


def _empty_file_inventory() -> CdcFileInventory:
    return CdcFileInventory(0, 0, 0, 0, 0, 0, 0, (), ())


def _bounded_snapshot_dict(result: SnapshotExpirationResult) -> dict[str, object]:
    """Serialize snapshot outcomes without unbounded IDs or path lists."""
    return {
        "table_name": result.table_name,
        "status": result.status,
        "retention_days": result.retention_days,
        "retain_last": result.retain_last,
        "snapshots_before": result.snapshots_before,
        "snapshots_after": result.snapshots_after,
        "age_eligible_snapshot_count": len(result.age_eligible_snapshot_ids),
        "age_eligible_snapshot_sample": list(result.age_eligible_snapshot_ids[:PATH_SAMPLE_LIMIT]),
        "protected_snapshot_count": len(result.protected_snapshot_ids),
        "protected_snapshot_sample": list(result.protected_snapshot_ids[:PATH_SAMPLE_LIMIT]),
        "removed_snapshot_count": len(result.removed_snapshot_ids),
        "removed_snapshot_sample": list(result.removed_snapshot_ids[:PATH_SAMPLE_LIMIT]),
        "removed_manifest_count": result.removed_manifest_count,
        "removed_data_file_count": result.removed_data_file_count,
        "removed_manifest_sample": list(result.removed_manifest_sample),
        "removed_data_file_sample": list(result.removed_data_file_sample),
        "concurrent_main_change": result.concurrent_main_change,
    }


def _empty_snapshot_result(
    status: Literal["skipped_missing", "no_op"], policy: CdcMaintenancePolicy
) -> SnapshotExpirationResult:
    return SnapshotExpirationResult(
        table_name=CDC_TABLE,
        status=status,
        retention_days=policy.snapshot_retention_days,
        retain_last=policy.snapshot_retain_last,
        snapshots_before=0,
        snapshots_after=0,
        age_eligible_snapshot_ids=(),
        protected_snapshot_ids=(),
        removed_snapshot_ids=(),
        removed_manifest_count=0,
        removed_data_file_count=0,
        removed_manifest_sample=(),
        removed_data_file_sample=(),
    )
