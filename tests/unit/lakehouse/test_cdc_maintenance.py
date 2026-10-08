"""Unit contracts for bounded CDC Iceberg maintenance."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta

import pytest

from omni_retail.lakehouse import maintenance
from omni_retail.lakehouse.cdc_maintenance import (  # pyright: ignore[reportMissingImports]
    CDC_TABLES,
    CdcMaintenanceError,
    CdcMaintenancePolicy,
    _parse_optimize_metrics,
    compact_cdc_data_files,
    expire_cdc_snapshots,
    plan_cdc_maintenance,
)
from omni_retail.streaming.cdc.sql import CDC_TABLE

NOW = datetime.now(UTC)
EVENT_DATE = date(2026, 10, 1)


class StatefulCdcExecutor:
    def __init__(
        self,
        *,
        exists: bool = True,
        duplicate: bool = False,
        malformed_files: bool = False,
        lose_row: bool = False,
        concurrent_append: bool = False,
    ) -> None:
        self.exists = exists
        self.duplicate = duplicate
        self.malformed_files = malformed_files
        self.lose_row = lose_row
        self.concurrent_append = concurrent_append
        self.optimized = False
        self.expired = False
        self.statements: list[str] = []

    def close(self) -> None:
        pass

    def execute(self, sql: str) -> None:
        self.statements.append(sql)
        if "EXECUTE expire_snapshots" in sql:
            self.expired = True

    def fetch(self, sql: str) -> list[tuple[object, ...]]:
        self.statements.append(sql)
        if "information_schema.tables" in sql:
            return [(1 if self.exists else 0,)]
        if "EXECUTE optimize" in sql:
            self.optimized = True
            return [
                ("rewritten_data_files_count", "2"),
                ("removed_delete_files_count", "0"),
                ("added_data_files_count", "1"),
            ]
        if '"postgres_cdc_events$files"' in sql:
            if self.malformed_files:
                return [(EVENT_DATE, "bad", -1, 10)]
            if self.optimized:
                rows: list[tuple[object, ...]] = [
                    (EVENT_DATE, "s3://lakehouse/compacted.parquet", 2, 180),
                    (date(2026, 10, 2), "s3://lakehouse/f3.parquet", 1, 100),
                ]
                if self.concurrent_append:
                    rows.append((date(2026, 10, 2), "s3://lakehouse/new.parquet", 1, 100))
                return rows
            return [
                (EVENT_DATE, "s3://lakehouse/f1.parquet", 1, 100),
                (EVENT_DATE, "s3://lakehouse/f2.parquet", 1, 100),
                (date(2026, 10, 2), "s3://lakehouse/f3.parquet", 1, 100),
            ]
        if "count(DISTINCT event_id)" in sql:
            identity_rows = 4 if self.optimized and self.concurrent_append else 3
            distinct = identity_rows - 1 if self.duplicate else identity_rows
            return [(identity_rows, distinct)]
        if "missing_or_changed" in sql:
            return [(1 if self.lose_row else 0,)]
        if "FOR VERSION AS OF" in sql and "count(*)" in sql:
            return [(3,)]
        if "JOIN" in sql and '"postgres_cdc_events$refs"' in sql:
            if not self.optimized:
                return [(10, 9)]
            if self.concurrent_append:
                return [(12, 11)]
            return [(11, 10)]
        if 'SELECT count(*) FROM "iceberg"."bronze"."postgres_cdc_events$snapshots"' in sql:
            return [(2 if self.expired else 4,)]
        if '"postgres_cdc_events$snapshots"' in sql:
            snapshots: list[tuple[object, ...]] = [
                (NOW - timedelta(days=60), 7, None, "append", "m7"),
                (NOW - timedelta(days=40), 8, 7, "append", "m8"),
                (NOW - timedelta(days=2), 9, 8, "append", "m9"),
                (NOW - timedelta(days=1), 10, 9, "append", "m10"),
            ]
            return snapshots[2:] if self.expired else snapshots
        if '"postgres_cdc_events$refs"' in sql:
            return [("main", "BRANCH", 10)]
        if '"postgres_cdc_events$all_manifests"' in sql:
            values = ("m9", "m10") if self.expired else ("m7", "m8", "m9", "m10")
            return [(value,) for value in values]
        if '"postgres_cdc_events$all_entries"' in sql:
            values = ("f9", "f10") if self.expired else ("f7", "f8", "f9", "f10")
            return [(value,) for value in values]
        raise AssertionError(f"unexpected SQL: {sql}")


def test_policy_defaults_environment_and_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "ICEBERG_CDC_FILE_SIZE_THRESHOLD_MB",
        "ICEBERG_CDC_SNAPSHOT_RETENTION_DAYS",
        "ICEBERG_CDC_SNAPSHOT_RETAIN_LAST",
    ):
        monkeypatch.delenv(name, raising=False)
    assert CdcMaintenancePolicy.from_env() == CdcMaintenancePolicy(128, 30, 10)

    monkeypatch.setenv("ICEBERG_CDC_FILE_SIZE_THRESHOLD_MB", "64")
    monkeypatch.setenv("ICEBERG_CDC_SNAPSHOT_RETENTION_DAYS", "45")
    monkeypatch.setenv("ICEBERG_CDC_SNAPSHOT_RETAIN_LAST", "12")
    assert CdcMaintenancePolicy.from_env() == CdcMaintenancePolicy(64, 45, 12)

    invalid_policies: tuple[Callable[[], CdcMaintenancePolicy], ...] = (
        lambda: CdcMaintenancePolicy(0, 30, 10),
        lambda: CdcMaintenancePolicy(513, 30, 10),
        lambda: CdcMaintenancePolicy(128, 6, 10),
        lambda: CdcMaintenancePolicy(128, 30, 1),
        lambda: CdcMaintenancePolicy(True, 30, 10),
    )
    for policy in invalid_policies:
        with pytest.raises(ValueError):
            policy()


def test_policy_rejects_non_integer_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ICEBERG_CDC_FILE_SIZE_THRESHOLD_MB", "large")
    with pytest.raises(ValueError, match="must be an integer"):
        CdcMaintenancePolicy.from_env()


def test_singleton_allowlist_is_exact() -> None:
    assert CDC_TABLES == (CDC_TABLE,)


def test_missing_table_is_an_explicit_skip() -> None:
    executor = StatefulCdcExecutor(exists=False)

    plan = plan_cdc_maintenance(executor, CdcMaintenancePolicy())
    compaction = compact_cdc_data_files(executor, CdcMaintenancePolicy())
    expiration = expire_cdc_snapshots(executor, CdcMaintenancePolicy())

    assert plan.status == "skipped_missing"
    assert compaction.status == "skipped_missing"
    assert expiration.snapshot_result.status == "skipped_missing"
    assert not any("$files" in sql or "EXECUTE" in sql for sql in executor.statements)


def test_plan_is_read_only_and_reports_candidate_partition_and_snapshots() -> None:
    executor = StatefulCdcExecutor()

    result = plan_cdc_maintenance(executor, CdcMaintenancePolicy(1, 30, 2))

    assert result.status == "planned"
    assert result.files.files == 3
    assert result.files.candidate_files == 2
    assert [item.event_date for item in result.files.candidate_partitions] == [EVENT_DATE]
    assert result.snapshot_plan is not None
    assert result.snapshot_plan.age_eligible_snapshot_ids == (7, 8)
    assert result.current_rows == result.distinct_event_ids == 3
    assert not any(
        "EXECUTE optimize" in sql or "EXECUTE expire" in sql for sql in executor.statements
    )
    assert result.as_dict()["files"] == result.files.as_dict()


def test_compaction_uses_exact_sql_parses_metrics_and_preserves_rows() -> None:
    executor = StatefulCdcExecutor()

    result = compact_cdc_data_files(executor, CdcMaintenancePolicy(1, 30, 2))

    assert result.status == "compacted"
    assert result.before.files == 3
    assert result.after.files == 2
    assert result.metrics.rewritten_data_files_count == 2
    assert result.metrics.added_data_files_count == 1
    assert result.preservation is not None
    assert result.preservation.snapshot_rows == result.preservation.current_rows == 3
    assert result.main_snapshot_before == 10
    assert result.main_snapshot_after == 11
    assert result.concurrent_main_change is False
    assert (
        'ALTER TABLE "iceberg"."bronze"."postgres_cdc_events" EXECUTE optimize('
        "file_size_threshold => '1MB')"
    ) in executor.statements


def test_compaction_allows_concurrent_appends_but_reports_main_change() -> None:
    executor = StatefulCdcExecutor(concurrent_append=True)

    result = compact_cdc_data_files(executor, CdcMaintenancePolicy(1, 30, 2))

    assert result.preservation is not None
    assert result.preservation.snapshot_rows == 3
    assert result.preservation.current_rows == 4
    assert result.preservation.distinct_event_ids == 4
    assert result.concurrent_main_change is True


def test_duplicate_event_id_fails_before_destructive_work() -> None:
    executor = StatefulCdcExecutor(duplicate=True)

    with pytest.raises(CdcMaintenanceError, match="event_id uniqueness failed"):
        compact_cdc_data_files(executor, CdcMaintenancePolicy(1, 30, 2))

    assert not any("EXECUTE optimize" in sql for sql in executor.statements)


def test_malformed_metadata_fails_before_destructive_work() -> None:
    executor = StatefulCdcExecutor(malformed_files=True)

    with pytest.raises(CdcMaintenanceError, match="invalid CDC data-file metadata"):
        compact_cdc_data_files(executor, CdcMaintenancePolicy(1, 30, 2))

    assert not any("EXECUTE optimize" in sql for sql in executor.statements)


def test_changed_or_missing_preexisting_row_fails_postcondition() -> None:
    executor = StatefulCdcExecutor(lose_row=True)

    with pytest.raises(CdcMaintenanceError, match="raw-row preservation failed"):
        compact_cdc_data_files(executor, CdcMaintenancePolicy(1, 30, 2))


def test_optimize_metric_parser_is_strict() -> None:
    with pytest.raises(CdcMaintenanceError, match="match exactly"):
        _parse_optimize_metrics([("rewritten_data_files_count", 2)])
    with pytest.raises(CdcMaintenanceError, match="unexpected or duplicate"):
        _parse_optimize_metrics(
            [
                ("rewritten_data_files_count", 2),
                ("removed_delete_files_count", 0),
                ("surprise", 1),
            ]
        )
    with pytest.raises(CdcMaintenanceError, match="disagree"):
        _parse_optimize_metrics(
            [
                ("rewritten_data_files_count", 2),
                ("removed_delete_files_count", 0),
                ("added_data_files_count", 0),
            ]
        )


def test_expiration_reuses_ref_protection_and_preserves_current_rows() -> None:
    executor = StatefulCdcExecutor()

    result = expire_cdc_snapshots(executor, CdcMaintenancePolicy(1, 30, 2))

    assert result.snapshot_result.status == "expired"
    assert result.snapshot_result.protected_snapshot_ids == (9, 10)
    assert result.snapshot_result.removed_snapshot_ids == (7, 8)
    assert result.preservation is not None
    assert result.preservation.missing_or_changed_rows == 0
    assert (
        'ALTER TABLE "iceberg"."bronze"."postgres_cdc_events" '
        "EXECUTE expire_snapshots(retention_threshold => '30d', retain_last => 2, "
        "clean_expired_metadata => false)"
    ) in executor.statements


def test_cdc_apply_failure_is_nonzero_and_stops_later_stage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = StatefulCdcExecutor()

    def fail_compaction(*args: object, **kwargs: object) -> object:
        raise CdcMaintenanceError("simulated optimize conflict")

    monkeypatch.setattr(maintenance, "DbapiTrinoExecutor", lambda config: executor)
    monkeypatch.setattr(maintenance, "compact_cdc_data_files", fail_compaction)
    monkeypatch.setattr(
        maintenance,
        "expire_cdc_snapshots",
        lambda *args, **kwargs: pytest.fail("expiration must not follow failed compaction"),
    )

    assert maintenance.main(["cdc-apply", "--confirm"]) == 1


def test_cdc_apply_requires_confirmation_before_connecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        maintenance,
        "DbapiTrinoExecutor",
        lambda config: pytest.fail("Trino must not be opened without --confirm"),
    )

    assert maintenance.main(["cdc-apply"]) == 2
