# Iceberg batch Bronze snapshot maintenance

Use this runbook to inspect or expire old snapshot history for the fourteen
batch-owned Iceberg Bronze tables. The maintenance path does not target the raw
CDC ledger, dbt-managed schemas, or unknown tables.

Snapshot expiration is irreversible. The retained window supports recent time
travel and rollback; recovery of older batch Bronze state requires a rebuild
from the immutable raw archive.

## Policy and scope

Production defaults:

| Setting | Default | Hard floor |
| --- | ---: | ---: |
| `ICEBERG_SNAPSHOT_RETENTION_DAYS` | 30 days | 7 days |
| `ICEBERG_SNAPSHOT_RETAIN_LAST` | 10 ancestor snapshots | 2 snapshots |

Both settings must be integers. Application validation rejects values below the
floors before connecting to Trino. The Trino catalog independently enforces
`iceberg.expire-snapshots.min-retention=7d`.

Targets come only from the static batch table registry in
`src/omni_retail/lakehouse/bronze/specs.py`. Missing tables—for example a
supplier table that has not been loaded yet—are reported as `skipped_missing`.
The following are intentionally excluded:

- `bronze.postgres_cdc_events` (tracked separately by TD-006);
- Silver, Gold, analytics, and integration-test schemas;
- tables not present in the batch registry;
- orphan-file removal, data-file compaction, and manifest rewrite.

The procedure passes `clean_expired_metadata => false` and preserves the current
`main` snapshot, Iceberg refs, every snapshot younger than the retention
threshold, and at least `retain_last` ancestors.

## Safe preview

The core stack must be healthy. Preview every target without changing data:

```bash
make smoke-core
make iceberg-snapshot-plan
```

Preview one high-churn table:

```bash
make iceberg-snapshot-plan ARGS="--table order_items"
```

Review each status:

- `skipped_missing` — table has not been created; no action;
- `no_op` — no unprotected snapshot is old enough;
- `planned` — one or more snapshots are age-eligible; Iceberg refs can still
  protect additional history when apply runs.

The preview logs the table, retention policy, snapshot counts, eligible and
protected snapshot IDs, and no-op status. It never invokes
`expire_snapshots`.

## Apply

Do not overlap a host-side apply with `make bronze-load`, a Bronze rebuild, or
another writer to the same tables. Review the preview immediately before apply.

Apply one table first:

```bash
make iceberg-snapshot-expire ARGS="--table order_items"
```

Then apply every registered batch table if the targeted result is healthy:

```bash
make iceberg-snapshot-expire
```

The explicit target prints an irreversibility warning and supplies the CLI's
required confirmation flag. The operation continues across independent tables
and exits non-zero if any table fails. A retry replans metadata; already
expired history becomes a no-op.

Per-table logs include snapshots before/after, age-eligible and removed snapshot
counts, removed manifest/data-file counts, bounded path samples, and whether a
concurrent `main` change was observed. File samples are bounded to avoid
unbounded Airflow logs.

## Scheduled operation

The `maintain_iceberg_snapshots` Airflow DAG runs each Sunday at `03:00 UTC`,
with `catchup=False` and one independent task per registered table. New DAGs are
paused when first installed; review the environment policy before unpausing it.

The one-slot `iceberg_bronze` pool is shared with `load_bronze.load_new`. It
serializes scheduler-managed batch Bronze writes and expiration. It does not
lock host-side manual commands, so operators must still avoid concurrent runs.

Check DAG/task state with:

```bash
make airflow-up
docker compose exec airflow-scheduler airflow dags list | grep maintain_iceberg_snapshots
docker compose exec airflow-scheduler airflow tasks states-for-dag-run \
  maintain_iceberg_snapshots <dag-run-id>
```

## Verify

Inspect one table before and after apply:

```sql
SELECT committed_at, snapshot_id, parent_id, operation
FROM iceberg.bronze."order_items$snapshots"
ORDER BY committed_at DESC;

SELECT name, type, snapshot_id, min_snapshots_to_keep, max_snapshot_age_in_ms
FROM iceberg.bronze."order_items$refs"
ORDER BY name;
```

Expected results:

- `main` still points to the current table state;
- at least ten current ancestors remain under the default policy, unless the
  table has fewer than ten snapshots in total;
- snapshots newer than 30 days remain;
- a repeated plan/apply reports `no_op` when no additional history is eligible;
- current row counts and downstream dbt results are unchanged.

For a retained snapshot ID, validate time travel explicitly:

```sql
SELECT count(*)
FROM iceberg.bronze.orders
FOR VERSION AS OF <retained_snapshot_id>;
```

## Failure handling

The command fails loudly for unsafe configuration, unknown tables, malformed
metadata, a missing `main` ref, Trino/Polaris errors, or a failed postcondition.
Successful tables are not rolled back when another table fails.

1. Preserve the command or Airflow task logs.
2. Record the table, pre-run `main` snapshot ID, protected IDs, and any removed
   counts.
3. Check the core stack:

   ```bash
   docker compose --profile core ps
   docker compose --profile core logs --tail=200 trino polaris
   make smoke-core
   ```

4. Resolve the infrastructure problem and run the read-only plan again.
5. Retry only after confirming no batch Bronze writer is active.

Do not lower the catalog/application retention floors, run
`remove_orphan_files`, or delete MinIO paths manually as a recovery shortcut.

## Recovery beyond the retained window

An expired snapshot cannot be restored in place. Batch Bronze remains
rebuildable because raw archive objects and canonical manifests are not touched
by maintenance. Follow [Bronze full rebuild and recovery](bronze-rebuild.md):

```bash
make bronze-rebuild
make dbt-build
make serving-rebuild
```

A Bronze rebuild is destructive for the configured Bronze schema and must not
be used merely to retry normal snapshot maintenance.
