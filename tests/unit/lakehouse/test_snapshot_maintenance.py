"""Unit tests for bounded Iceberg snapshot expiration."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from omni_retail.lakehouse import maintenance
from omni_retail.lakehouse.snapshot_maintenance import (
    DEFAULT_RETAIN_LAST,
    DEFAULT_RETENTION_DAYS,
    SnapshotExpirationPolicy,
    SnapshotMaintenanceError,
    expire_table_snapshots,
    plan_table_snapshots,
)

NOW = datetime(2026, 10, 8, 12, tzinfo=UTC)


class StatefulExecutor:
    def __init__(self, *, exists: bool = True, recent_only: bool = False) -> None:
        self.exists = exists
        self.recent_only = recent_only
        self.expired = False
        self.statements: list[str] = []

    def execute(self, sql: str) -> None:
        self.statements.append(sql)
        if "EXECUTE expire_snapshots" in sql:
            self.expired = True

    def fetch(self, sql: str) -> list[tuple[object, ...]]:
        self.statements.append(sql)
        if "information_schema.tables" in sql:
            return [(1 if self.exists else 0,)]
        if '"orders$snapshots"' in sql:
            return self._snapshots()
        if '"orders$refs"' in sql:
            return [("main", "BRANCH", 4)]
        if '"orders$all_manifests"' in sql:
            paths = ("m3", "m4") if self.expired else ("m1", "m2", "m3", "m4")
            return [(path,) for path in paths]
        if '"orders$all_entries"' in sql:
            paths = ("f3", "f4") if self.expired else ("f1", "f2", "f3", "f4")
            return [(path,) for path in paths]
        raise AssertionError(f"unexpected SQL: {sql}")

    def _snapshots(self) -> list[tuple[object, ...]]:
        snapshots: list[tuple[object, ...]] = [
            (NOW - timedelta(days=40), 1, None, "append", "ml1"),
            (NOW - timedelta(days=35), 2, 1, "append", "ml2"),
            (NOW - timedelta(days=2), 3, 2, "append", "ml3"),
            (NOW - timedelta(days=1), 4, 3, "append", "ml4"),
        ]
        if self.recent_only:
            snapshots = [
                (NOW - timedelta(days=2), 3, None, "append", "ml3"),
                (NOW - timedelta(days=1), 4, 3, "append", "ml4"),
            ]
        if self.expired:
            snapshots = snapshots[2:]
        return snapshots


def test_policy_defaults_and_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ICEBERG_SNAPSHOT_RETENTION_DAYS", raising=False)
    monkeypatch.delenv("ICEBERG_SNAPSHOT_RETAIN_LAST", raising=False)
    assert SnapshotExpirationPolicy.from_env() == SnapshotExpirationPolicy(
        DEFAULT_RETENTION_DAYS, DEFAULT_RETAIN_LAST
    )

    monkeypatch.setenv("ICEBERG_SNAPSHOT_RETENTION_DAYS", "45")
    monkeypatch.setenv("ICEBERG_SNAPSHOT_RETAIN_LAST", "12")
    assert SnapshotExpirationPolicy.from_env() == SnapshotExpirationPolicy(45, 12)


@pytest.mark.parametrize(
    ("retention_days", "retain_last", "message"),
    [
        (6, 10, "at least 7 days"),
        (30, 1, "at least 2"),
        (True, 10, "at least 7 days"),
        (30, True, "at least 2"),
    ],
)
def test_policy_rejects_unsafe_values(retention_days: int, retain_last: int, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        SnapshotExpirationPolicy(retention_days, retain_last)


def test_policy_rejects_non_integer_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ICEBERG_SNAPSHOT_RETENTION_DAYS", "thirty")
    with pytest.raises(ValueError, match="must be an integer"):
        SnapshotExpirationPolicy.from_env()


def test_plan_reports_age_eligible_and_protected_snapshots() -> None:
    executor = StatefulExecutor()

    result = plan_table_snapshots(
        executor,
        "orders",
        SnapshotExpirationPolicy(retention_days=30, retain_last=2),
        clock=NOW,
    )

    assert result.status == "planned"
    assert result.age_eligible_snapshot_ids == (1, 2)
    assert result.protected_snapshot_ids == (3, 4)
    assert result.snapshots_before == result.snapshots_after == 4
    assert not any("EXECUTE expire_snapshots" in sql for sql in executor.statements)


def test_expire_uses_safe_named_arguments_and_reports_removed_files() -> None:
    executor = StatefulExecutor()

    result = expire_table_snapshots(
        executor,
        "orders",
        SnapshotExpirationPolicy(retention_days=30, retain_last=2),
        clock=NOW,
    )

    assert result.status == "expired"
    assert result.removed_snapshot_ids == (1, 2)
    assert result.protected_snapshot_ids == (3, 4)
    assert result.removed_manifest_count == 2
    assert result.removed_data_file_count == 2
    assert result.removed_manifest_sample == ("m1", "m2")
    assert result.removed_data_file_sample == ("f1", "f2")
    assert result.concurrent_main_change is False
    assert (
        'ALTER TABLE "iceberg"."bronze"."orders" EXECUTE expire_snapshots('
        "retention_threshold => '30d', retain_last => 2, "
        "clean_expired_metadata => false)"
    ) in executor.statements


def test_expire_is_noop_when_only_protected_recent_snapshots_exist() -> None:
    executor = StatefulExecutor(recent_only=True)

    result = expire_table_snapshots(
        executor,
        "orders",
        SnapshotExpirationPolicy(),
        clock=NOW,
    )

    assert result.status == "no_op"
    assert result.snapshots_before == result.snapshots_after == 2
    assert not any("EXECUTE expire_snapshots" in sql for sql in executor.statements)


def test_missing_registered_table_is_an_explicit_skip() -> None:
    executor = StatefulExecutor(exists=False)

    result = plan_table_snapshots(executor, "orders", SnapshotExpirationPolicy(), clock=NOW)

    assert result.status == "skipped_missing"
    assert result.snapshots_before == 0


def test_unknown_table_fails_before_query() -> None:
    executor = StatefulExecutor()

    with pytest.raises(ValueError, match="unknown batch Bronze table"):
        plan_table_snapshots(executor, "postgres_cdc_events", SnapshotExpirationPolicy())

    assert executor.statements == []


def test_missing_main_ref_fails_loudly() -> None:
    executor = StatefulExecutor()
    original_fetch = executor.fetch

    def fetch_without_main(sql: str) -> list[tuple[object, ...]]:
        if '"orders$refs"' in sql:
            return []
        return original_fetch(sql)

    executor.fetch = fetch_without_main  # type: ignore[method-assign]
    with pytest.raises(SnapshotMaintenanceError, match="exactly one main ref"):
        plan_table_snapshots(executor, "orders", SnapshotExpirationPolicy(), clock=NOW)


def test_cli_requires_confirmation_before_connecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        maintenance,
        "DbapiTrinoExecutor",
        lambda config: pytest.fail("Trino must not be opened without --confirm"),
    )

    assert maintenance.main(["expire", "--table", "orders"]) == 2
