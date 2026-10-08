"""Pinned Trino/Iceberg compatibility proof for snapshot expiration."""

from __future__ import annotations

import contextlib
import time

from integration.conftest import LakehouseNamespace
from omni_retail.lakehouse.trino import DbapiTrinoExecutor, TrinoConfig


def test_trino_483_expiration_preserves_main_and_two_rollback_snapshots(
    lakehouse_namespace: LakehouseNamespace,
) -> None:
    """Exercise unsafe-short retention only inside an owned disposable schema."""
    schema = lakehouse_namespace.bronze
    table = f'iceberg."{schema}".orders'
    with contextlib.closing(DbapiTrinoExecutor(TrinoConfig.from_env())) as executor:
        executor.execute(f"CREATE TABLE {table} (order_id bigint)")
        executor.execute(f"INSERT INTO {table} VALUES (1)")
        executor.execute(f"DELETE FROM {table} WHERE order_id = 1")
        executor.execute(f"INSERT INTO {table} VALUES (2)")
        executor.execute(f"INSERT INTO {table} VALUES (3)")

        before_files = executor.fetch(
            f'SELECT DISTINCT data_file.file_path FROM iceberg."{schema}"."orders$all_entries"'
        )
        before_refs = executor.fetch(
            f'SELECT name, type, snapshot_id FROM iceberg."{schema}"."orders$refs"'
        )
        assert len(before_refs) == 1
        assert before_refs[0][0] == "main"
        assert before_refs[0][1] == "BRANCH"
        before_main = before_refs[0][2]
        assert isinstance(before_main, int)

        # Wait on the connector clock predicate rather than assuming that the
        # latest millisecond-resolution commit is immediately older than now.
        for _ in range(20):
            is_older = executor.fetch(
                "SELECT max(committed_at) < current_timestamp "
                f'FROM iceberg."{schema}"."orders$snapshots"'
            )[0][0]
            if bool(is_older):
                break
            time.sleep(0.01)
        else:
            raise AssertionError("snapshot commit time did not become eligible")

        # Test-only session override from Trino's own connector tests. Runtime
        # policy and catalog configuration retain their seven-day hard floor.
        executor.execute("SET SESSION iceberg.expire_snapshots_min_retention = '0s'")
        executor.execute(
            f"ALTER TABLE {table} EXECUTE expire_snapshots("
            "retention_threshold => '0s', retain_last => 2, "
            "clean_expired_metadata => false)"
        )

        after_rows = executor.fetch(
            f'SELECT snapshot_id, parent_id FROM iceberg."{schema}"."orders$snapshots"'
        )
        assert len(after_rows) == 2
        parent_by_snapshot: dict[int, object] = {}
        for snapshot_id, parent_id in after_rows:
            assert isinstance(snapshot_id, int)
            parent_by_snapshot[snapshot_id] = parent_id
        assert before_main in parent_by_snapshot
        previous_snapshot = parent_by_snapshot[before_main]
        assert isinstance(previous_snapshot, int)
        assert previous_snapshot in parent_by_snapshot

        after_files = executor.fetch(
            f'SELECT DISTINCT data_file.file_path FROM iceberg."{schema}"."orders$all_entries"'
        )
        before_paths = {row[0] for row in before_files}
        after_paths = {row[0] for row in after_files}
        assert after_paths <= before_paths

        current_rows = executor.fetch(f"SELECT array_agg(order_id ORDER BY order_id) FROM {table}")
        assert len(current_rows) == 1
        assert current_rows[0][0] == [2, 3]
        previous_rows = executor.fetch(
            f"SELECT array_agg(order_id ORDER BY order_id) FROM {table} "
            f"FOR VERSION AS OF {previous_snapshot}"
        )
        assert len(previous_rows) == 1
        assert previous_rows[0][0] == [2]

        after_refs = executor.fetch(
            f'SELECT name, type, snapshot_id FROM iceberg."{schema}"."orders$refs"'
        )
        assert after_refs == before_refs

        executor.execute(
            f"ALTER TABLE {table} EXECUTE expire_snapshots("
            "retention_threshold => '0s', retain_last => 2, "
            "clean_expired_metadata => false)"
        )
        remaining = executor.fetch(f'SELECT count(*) FROM iceberg."{schema}"."orders$snapshots"')
        assert len(remaining) == 1
        assert remaining[0][0] == 2
