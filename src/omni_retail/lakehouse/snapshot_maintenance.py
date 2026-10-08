"""Safe planning and expiration of batch Bronze Iceberg snapshots."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

from omni_retail.lakehouse.bronze.specs import TABLES
from omni_retail.lakehouse.trino import TrinoExecutor, validate_schema_name

DEFAULT_RETENTION_DAYS = 30
DEFAULT_RETAIN_LAST = 10
MIN_RETENTION_DAYS = 7
MIN_RETAIN_LAST = 2
PATH_SAMPLE_LIMIT = 20

ExpirationStatus = Literal["skipped_missing", "no_op", "planned", "expired"]


class SnapshotMaintenanceError(RuntimeError):
    """Snapshot metadata or an expiration postcondition is unsafe."""


@dataclass(frozen=True)
class SnapshotExpirationPolicy:
    """Validated snapshot retention settings used by CLI and Airflow."""

    retention_days: int = DEFAULT_RETENTION_DAYS
    retain_last: int = DEFAULT_RETAIN_LAST

    def __post_init__(self) -> None:
        if type(self.retention_days) is not int or self.retention_days < MIN_RETENTION_DAYS:
            raise ValueError(f"snapshot retention must be at least {MIN_RETENTION_DAYS} days")
        if type(self.retain_last) is not int or self.retain_last < MIN_RETAIN_LAST:
            raise ValueError(f"snapshot retain_last must be at least {MIN_RETAIN_LAST}")

    @classmethod
    def from_env(cls) -> SnapshotExpirationPolicy:
        """Resolve non-secret policy values from the process environment."""
        return cls(
            retention_days=_environment_integer(
                "ICEBERG_SNAPSHOT_RETENTION_DAYS", DEFAULT_RETENTION_DAYS
            ),
            retain_last=_environment_integer("ICEBERG_SNAPSHOT_RETAIN_LAST", DEFAULT_RETAIN_LAST),
        )


@dataclass(frozen=True)
class SnapshotRecord:
    committed_at: datetime
    snapshot_id: int
    parent_id: int | None
    operation: str
    manifest_list: str


@dataclass(frozen=True)
class SnapshotRef:
    name: str
    ref_type: str
    snapshot_id: int


@dataclass(frozen=True)
class SnapshotInventory:
    snapshots: tuple[SnapshotRecord, ...]
    refs: tuple[SnapshotRef, ...]
    manifest_paths: frozenset[str]
    data_file_paths: frozenset[str]

    @property
    def main_snapshot_id(self) -> int:
        matches = [ref.snapshot_id for ref in self.refs if ref.name == "main"]
        if len(matches) != 1:
            raise SnapshotMaintenanceError(
                f"expected exactly one main ref, observed {len(matches)}"
            )
        return matches[0]


@dataclass(frozen=True)
class SnapshotExpirationResult:
    table_name: str
    status: ExpirationStatus
    retention_days: int
    retain_last: int
    snapshots_before: int
    snapshots_after: int
    age_eligible_snapshot_ids: tuple[int, ...]
    protected_snapshot_ids: tuple[int, ...]
    removed_snapshot_ids: tuple[int, ...]
    removed_manifest_count: int
    removed_data_file_count: int
    removed_manifest_sample: tuple[str, ...]
    removed_data_file_sample: tuple[str, ...]
    concurrent_main_change: bool = False

    def as_dict(self) -> dict[str, object]:
        """Return an Airflow/XCom- and JSON-friendly result."""
        return {
            "table_name": self.table_name,
            "status": self.status,
            "retention_days": self.retention_days,
            "retain_last": self.retain_last,
            "snapshots_before": self.snapshots_before,
            "snapshots_after": self.snapshots_after,
            "age_eligible_snapshot_ids": list(self.age_eligible_snapshot_ids),
            "protected_snapshot_ids": list(self.protected_snapshot_ids),
            "removed_snapshot_ids": list(self.removed_snapshot_ids),
            "removed_manifest_count": self.removed_manifest_count,
            "removed_data_file_count": self.removed_data_file_count,
            "removed_manifest_sample": list(self.removed_manifest_sample),
            "removed_data_file_sample": list(self.removed_data_file_sample),
            "concurrent_main_change": self.concurrent_main_change,
        }


def plan_table_snapshots(
    executor: TrinoExecutor,
    table_name: str,
    policy: SnapshotExpirationPolicy,
    *,
    catalog: str = "iceberg",
    schema: str = "bronze",
    clock: datetime | None = None,
) -> SnapshotExpirationResult:
    """Read metadata and report snapshots eligible by age without mutating the table."""
    _validate_target(table_name, catalog, schema)
    if not _table_exists(executor, table_name, catalog, schema):
        return _empty_result(table_name, "skipped_missing", policy)

    inventory = _read_inventory(executor, table_name, catalog, schema)
    protected = _protected_snapshot_ids(inventory, policy.retain_last)
    eligible = _age_eligible_snapshot_ids(inventory, policy, clock=clock)
    return SnapshotExpirationResult(
        table_name=table_name,
        status="planned" if eligible else "no_op",
        retention_days=policy.retention_days,
        retain_last=policy.retain_last,
        snapshots_before=len(inventory.snapshots),
        snapshots_after=len(inventory.snapshots),
        age_eligible_snapshot_ids=eligible,
        protected_snapshot_ids=protected,
        removed_snapshot_ids=(),
        removed_manifest_count=0,
        removed_data_file_count=0,
        removed_manifest_sample=(),
        removed_data_file_sample=(),
    )


def expire_table_snapshots(
    executor: TrinoExecutor,
    table_name: str,
    policy: SnapshotExpirationPolicy,
    *,
    catalog: str = "iceberg",
    schema: str = "bronze",
    clock: datetime | None = None,
) -> SnapshotExpirationResult:
    """Expire eligible snapshots and verify refs and rollback snapshots remain."""
    _validate_target(table_name, catalog, schema)
    if not _table_exists(executor, table_name, catalog, schema):
        return _empty_result(table_name, "skipped_missing", policy)

    before = _read_inventory(executor, table_name, catalog, schema)
    protected = _protected_snapshot_ids(before, policy.retain_last)
    eligible = _age_eligible_snapshot_ids(before, policy, clock=clock)
    if not eligible:
        return SnapshotExpirationResult(
            table_name=table_name,
            status="no_op",
            retention_days=policy.retention_days,
            retain_last=policy.retain_last,
            snapshots_before=len(before.snapshots),
            snapshots_after=len(before.snapshots),
            age_eligible_snapshot_ids=(),
            protected_snapshot_ids=protected,
            removed_snapshot_ids=(),
            removed_manifest_count=0,
            removed_data_file_count=0,
            removed_manifest_sample=(),
            removed_data_file_sample=(),
        )

    # pi-lens-ignore: python-sql-injection -- identifiers are regex-validated and table-allowlisted
    executor.execute(_expire_sql(table_name, policy, catalog, schema))
    after = _read_inventory(executor, table_name, catalog, schema)
    after_snapshot_ids = {snapshot.snapshot_id for snapshot in after.snapshots}
    missing_protected = set(protected) - after_snapshot_ids
    if missing_protected:
        raise SnapshotMaintenanceError(
            f"{table_name}: protected snapshots disappeared: {sorted(missing_protected)}"
        )

    before_non_main_refs = {
        (ref.name, ref.ref_type, ref.snapshot_id) for ref in before.refs if ref.name != "main"
    }
    after_non_main_refs = {
        (ref.name, ref.ref_type, ref.snapshot_id) for ref in after.refs if ref.name != "main"
    }
    missing_refs = before_non_main_refs - after_non_main_refs
    if missing_refs:
        raise SnapshotMaintenanceError(f"{table_name}: protected refs disappeared: {missing_refs}")

    removed_snapshots = tuple(
        sorted({snapshot.snapshot_id for snapshot in before.snapshots} - after_snapshot_ids)
    )
    removed_manifests = before.manifest_paths - after.manifest_paths
    removed_data_files = before.data_file_paths - after.data_file_paths
    concurrent_main_change = before.main_snapshot_id != after.main_snapshot_id
    return SnapshotExpirationResult(
        table_name=table_name,
        status="expired" if removed_snapshots else "no_op",
        retention_days=policy.retention_days,
        retain_last=policy.retain_last,
        snapshots_before=len(before.snapshots),
        snapshots_after=len(after.snapshots),
        age_eligible_snapshot_ids=eligible,
        protected_snapshot_ids=protected,
        removed_snapshot_ids=removed_snapshots,
        removed_manifest_count=len(removed_manifests),
        removed_data_file_count=len(removed_data_files),
        removed_manifest_sample=_path_sample(removed_manifests),
        removed_data_file_sample=_path_sample(removed_data_files),
        concurrent_main_change=concurrent_main_change,
    )


def _environment_integer(name: str, default: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        return int(raw)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from error


def _validate_target(table_name: str, catalog: str, schema: str) -> None:
    validate_schema_name(catalog)
    validate_schema_name(schema)
    if table_name not in TABLES:
        known = ", ".join(TABLES)
        raise ValueError(f"unknown batch Bronze table {table_name!r} (known: {known})")


def _identifier(value: str) -> str:
    return f'"{value}"'


def _qualified_table(table_name: str, catalog: str, schema: str) -> str:
    return ".".join(_identifier(value) for value in (catalog, schema, table_name))


def _metadata_table(table_name: str, suffix: str, catalog: str, schema: str) -> str:
    return ".".join(
        (_identifier(catalog), _identifier(schema), _identifier(f"{table_name}${suffix}"))
    )


def _table_exists(executor: TrinoExecutor, table_name: str, catalog: str, schema: str) -> bool:
    rows = executor.fetch(
        f"SELECT count(*) FROM {_identifier(catalog)}.information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_name = '{table_name}'"
    )
    if len(rows) != 1 or len(rows[0]) != 1 or not isinstance(rows[0][0], int):
        raise SnapshotMaintenanceError(f"{table_name}: malformed table-existence result")
    return rows[0][0] == 1


def _read_inventory(
    executor: TrinoExecutor, table_name: str, catalog: str, schema: str
) -> SnapshotInventory:
    snapshot_rows = executor.fetch(
        "SELECT committed_at, snapshot_id, parent_id, operation, manifest_list "
        f"FROM {_metadata_table(table_name, 'snapshots', catalog, schema)} "
        "ORDER BY committed_at, snapshot_id"
    )
    snapshots = tuple(_snapshot_record(table_name, row) for row in snapshot_rows)
    if not snapshots:
        raise SnapshotMaintenanceError(f"{table_name}: table has no snapshots")

    ref_rows = executor.fetch(
        "SELECT name, type, snapshot_id "
        f"FROM {_metadata_table(table_name, 'refs', catalog, schema)} ORDER BY name"
    )
    refs = tuple(_snapshot_ref(table_name, row) for row in ref_rows)
    inventory = SnapshotInventory(
        snapshots=snapshots,
        refs=refs,
        manifest_paths=_single_string_column(
            table_name,
            "manifest",
            executor.fetch(
                "SELECT DISTINCT path "
                f"FROM {_metadata_table(table_name, 'all_manifests', catalog, schema)}"
            ),
        ),
        data_file_paths=_single_string_column(
            table_name,
            "data file",
            executor.fetch(
                "SELECT DISTINCT data_file.file_path "
                f"FROM {_metadata_table(table_name, 'all_entries', catalog, schema)}"
            ),
        ),
    )
    _ = inventory.main_snapshot_id
    return inventory


def _snapshot_record(table_name: str, row: tuple[object, ...]) -> SnapshotRecord:
    if len(row) != 5:
        raise SnapshotMaintenanceError(f"{table_name}: malformed snapshot metadata row")
    committed_at, snapshot_id, parent_id, operation, manifest_list = row
    if not isinstance(committed_at, datetime):
        raise SnapshotMaintenanceError(f"{table_name}: invalid snapshot committed_at")
    if not isinstance(snapshot_id, int) or (
        parent_id is not None and not isinstance(parent_id, int)
    ):
        raise SnapshotMaintenanceError(f"{table_name}: invalid snapshot identifiers")
    if not isinstance(operation, str) or not isinstance(manifest_list, str):
        raise SnapshotMaintenanceError(f"{table_name}: invalid snapshot attributes")
    normalized_time = (
        committed_at.replace(tzinfo=UTC) if committed_at.tzinfo is None else committed_at
    )
    return SnapshotRecord(normalized_time, snapshot_id, parent_id, operation, manifest_list)


def _snapshot_ref(table_name: str, row: tuple[object, ...]) -> SnapshotRef:
    if (
        len(row) != 3
        or not isinstance(row[0], str)
        or not isinstance(row[1], str)
        or not isinstance(row[2], int)
    ):
        raise SnapshotMaintenanceError(f"{table_name}: malformed snapshot ref row")
    return SnapshotRef(row[0], row[1], row[2])


def _single_string_column(
    table_name: str, label: str, rows: list[tuple[object, ...]]
) -> frozenset[str]:
    values: set[str] = set()
    for row in rows:
        if len(row) != 1 or not isinstance(row[0], str):
            raise SnapshotMaintenanceError(f"{table_name}: malformed {label} metadata row")
        values.add(row[0])
    return frozenset(values)


def _protected_snapshot_ids(inventory: SnapshotInventory, retain_last: int) -> tuple[int, ...]:
    by_id = {snapshot.snapshot_id: snapshot for snapshot in inventory.snapshots}
    protected = {ref.snapshot_id for ref in inventory.refs}
    snapshot_id: int | None = inventory.main_snapshot_id
    for _ in range(retain_last):
        if snapshot_id is None:
            break
        snapshot = by_id.get(snapshot_id)
        if snapshot is None:
            raise SnapshotMaintenanceError(
                f"main ancestry references missing snapshot {snapshot_id}"
            )
        protected.add(snapshot.snapshot_id)
        snapshot_id = snapshot.parent_id
    return tuple(sorted(protected))


def _age_eligible_snapshot_ids(
    inventory: SnapshotInventory,
    policy: SnapshotExpirationPolicy,
    *,
    clock: datetime | None,
) -> tuple[int, ...]:
    now = clock or datetime.now(UTC)
    if now.tzinfo is None:
        raise ValueError("snapshot maintenance clock must be timezone-aware")
    cutoff = now - timedelta(days=policy.retention_days)
    protected = set(_protected_snapshot_ids(inventory, policy.retain_last))
    return tuple(
        snapshot.snapshot_id
        for snapshot in inventory.snapshots
        if snapshot.committed_at < cutoff and snapshot.snapshot_id not in protected
    )


def _expire_sql(
    table_name: str,
    policy: SnapshotExpirationPolicy,
    catalog: str,
    schema: str,
) -> str:
    return (
        f"ALTER TABLE {_qualified_table(table_name, catalog, schema)} "
        "EXECUTE expire_snapshots("
        f"retention_threshold => '{policy.retention_days}d', "
        f"retain_last => {policy.retain_last}, clean_expired_metadata => false)"
    )


def _path_sample(paths: frozenset[str]) -> tuple[str, ...]:
    return tuple(sorted(paths)[:PATH_SAMPLE_LIMIT])


def _empty_result(
    table_name: str,
    status: Literal["skipped_missing"],
    policy: SnapshotExpirationPolicy,
) -> SnapshotExpirationResult:
    return SnapshotExpirationResult(
        table_name=table_name,
        status=status,
        retention_days=policy.retention_days,
        retain_last=policy.retain_last,
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
