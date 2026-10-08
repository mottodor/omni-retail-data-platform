"""Pinned Trino/Iceberg proof for CDC compaction and retained snapshot history."""

from __future__ import annotations

import contextlib
import time

from integration.conftest import LakehouseNamespace
from omni_retail.lakehouse.cdc_maintenance import (  # pyright: ignore[reportMissingImports]
    CdcMaintenancePolicy,
    compact_cdc_data_files,
    plan_cdc_maintenance,
)
from omni_retail.lakehouse.trino import DbapiTrinoExecutor, TrinoConfig
from omni_retail.streaming.cdc.sql import CDC_COLUMNS, create_cdc_table_sql


def _insert_event(
    executor: DbapiTrinoExecutor,
    *,
    schema: str,
    event_number: int,
    event_date: str,
) -> None:
    table = f'iceberg."{schema}".postgres_cdc_events'
    columns = ", ".join(f'"{name}"' for name, _ in CDC_COLUMNS)
    executor.execute(
        f"INSERT INTO {table} ({columns}) VALUES ("  # nosec B608 -- owned schema/integers
        f"'event-{event_number}', 'omni.oltp.public.orders', 0, {event_number}, "
        f"TIMESTAMP '2026-10-01 12:00:0{event_number} UTC', 'public', 'orders', 'c', "
        f"{1000 + event_number}, {2000 + event_number}, "
        f"TIMESTAMP '2026-10-01 12:00:0{event_number} UTC', "
        f"'{{\"order_id\":{event_number}}}', "
        f'\'{{"op":"c","after":{{"order_id":{event_number}}}}}\', '
        "NULL, "
        f"'{{\"order_id\":{event_number}}}', DATE '{event_date}', "
        f"TIMESTAMP '2026-10-01 12:01:0{event_number} UTC')"
    )


def _all_rows(executor: DbapiTrinoExecutor, *, schema: str) -> list[tuple[object, ...]]:
    columns = ", ".join(f'"{name}"' for name, _ in CDC_COLUMNS)
    return executor.fetch(
        f'SELECT {columns} FROM iceberg."{schema}".postgres_cdc_events ORDER BY event_id'
    )


def test_trino_483_cdc_compaction_preserves_full_rows_and_repeat_is_noop(
    lakehouse_namespace: LakehouseNamespace,
) -> None:
    schema = lakehouse_namespace.bronze
    policy = CdcMaintenancePolicy(
        file_size_threshold_mb=1,
        snapshot_retention_days=7,
        snapshot_retain_last=2,
    )
    with contextlib.closing(DbapiTrinoExecutor(TrinoConfig.from_env())) as executor:
        # pi-lens-ignore: python-sql-injection -- shared builder validates owned schema
        executor.execute(create_cdc_table_sql(schema=schema))
        for event_number in range(1, 5):
            _insert_event(
                executor,
                schema=schema,
                event_number=event_number,
                event_date="2026-10-01",
            )
        _insert_event(executor, schema=schema, event_number=5, event_date="2026-10-02")

        before_rows = _all_rows(executor, schema=schema)
        assert len(before_rows) == 5
        first = compact_cdc_data_files(executor, policy, schema=schema)
        assert first.status == "compacted"
        assert first.metrics.rewritten_data_files_count >= 4
        assert first.metrics.added_data_files_count >= 1
        assert first.after.files < first.before.files
        assert _all_rows(executor, schema=schema) == before_rows
        assert first.preservation is not None
        assert first.preservation.current_rows == first.preservation.distinct_event_ids == 5

        repeated = compact_cdc_data_files(executor, policy, schema=schema)
        assert repeated.status == "no_op"
        assert repeated.metrics.rewritten_data_files_count == 0
        assert repeated.before.files == repeated.after.files

        preview = plan_cdc_maintenance(executor, policy, schema=schema)
        assert preview.current_rows == 5
        _insert_event(executor, schema=schema, event_number=6, event_date="2026-10-02")
        after_append = compact_cdc_data_files(executor, policy, schema=schema)
        assert after_append.status == "compacted"
        assert after_append.preservation is not None
        assert after_append.preservation.current_rows == 6
        assert after_append.preservation.distinct_event_ids == 6
        all_six_rows = _all_rows(executor, schema=schema)
        assert [row[0] for row in all_six_rows] == [f"event-{number}" for number in range(1, 7)]

        before_refs = executor.fetch(
            f'SELECT name, type, snapshot_id FROM iceberg."{schema}".'
            '"postgres_cdc_events$refs" ORDER BY name'
        )
        assert len(before_refs) == 1 and before_refs[0][0] == "main"
        before_main = before_refs[0][2]
        assert isinstance(before_main, int)

        for _ in range(20):
            eligible = executor.fetch(
                "SELECT max(committed_at) < current_timestamp "
                f'FROM iceberg."{schema}"."postgres_cdc_events$snapshots"'
            )[0][0]
            if bool(eligible):
                break
            time.sleep(0.01)
        else:
            raise AssertionError("CDC snapshot commit time did not become eligible")

        # Test-only override; runtime policy and catalog keep the seven-day floor.
        executor.execute("SET SESSION iceberg.expire_snapshots_min_retention = '0s'")
        executor.execute(
            f'ALTER TABLE iceberg."{schema}".postgres_cdc_events '
            "EXECUTE expire_snapshots(retention_threshold => '0s', retain_last => 2, "
            "clean_expired_metadata => false)"
        )
        snapshots = executor.fetch(
            f'SELECT snapshot_id, parent_id FROM iceberg."{schema}"."postgres_cdc_events$snapshots"'
        )
        assert len(snapshots) == 2
        parent_by_snapshot = {row[0]: row[1] for row in snapshots}
        assert before_main in parent_by_snapshot
        previous = parent_by_snapshot[before_main]
        assert isinstance(previous, int) and previous in parent_by_snapshot
        assert _all_rows(executor, schema=schema) == all_six_rows
        previous_count = executor.fetch(
            f'SELECT count(*) FROM iceberg."{schema}".postgres_cdc_events '
            f"FOR VERSION AS OF {previous}"
        )[0][0]
        assert previous_count in (5, 6)
        after_refs = executor.fetch(
            f'SELECT name, type, snapshot_id FROM iceberg."{schema}".'
            '"postgres_cdc_events$refs" ORDER BY name'
        )
        assert after_refs == before_refs
