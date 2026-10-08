# Iceberg CDC maintenance

Use this runbook to preview, compact, and expire physical Iceberg history for
exactly `iceberg.bronze.postgres_cdc_events`. Maintenance changes data files and
obsolete time-travel history; it never changes the current raw-event contract,
Kafka records, consumer-group offsets, table partitioning, or the consumer's
Iceberg-before-offset delivery boundary.

Snapshot expiration is irreversible beyond the retained rollback window.
Unlike batch Bronze, raw CDC events do not have an archive-object rebuild path.
Kafka retains data records for only seven days or 5 GiB per partition, so an
old expired Iceberg snapshot is not generally reconstructable.

## Policy and scope

| Setting | Default | Allowed / floor |
| --- | ---: | ---: |
| `ICEBERG_CDC_FILE_SIZE_THRESHOLD_MB` | 128 MiB | integer 1–512 MiB |
| `ICEBERG_CDC_SNAPSHOT_RETENTION_DAYS` | 30 days | at least 7 days |
| `ICEBERG_CDC_SNAPSHOT_RETAIN_LAST` | 10 ancestors | at least 2 ancestors |

The Trino catalog independently enforces a seven-day expiration floor.
Expiration uses `clean_expired_metadata => false`. The target is a singleton
allowlist; batch Bronze, dbt-managed schemas, unknown tables, manifest rewrite,
orphan-file removal, MinIO deletion, and raw-row TTL are not part of this path.
A missing CDC table is reported as `skipped_missing`.

Compaction calls Trino 483's Iceberg procedure:

```sql
ALTER TABLE iceberg.bronze.postgres_cdc_events
EXECUTE optimize(file_size_threshold => '128MB');
```

Iceberg optimizes independently per `event_date` partition. Preview identifies
partitions with at least two current files below the threshold; the connector
makes the final rewrite decision and returns rewritten/added file metrics.

## Read-only preview

Start with a healthy core and inspect CDC transport health when streaming is
running:

```bash
make smoke-core
make streaming-status
make iceberg-cdc-maintenance-plan
```

Preview never invokes `optimize` or `expire_snapshots`. Review:

- current rows and distinct event IDs;
- current file count, bytes, min/max size, and bounded file samples;
- candidate partitions/files;
- snapshot count, protected IDs, and age-eligible IDs;
- validated threshold and retention settings.

Any duplicate event ID, malformed Iceberg metadata, missing `main` ref, invalid
configuration, or Trino/Polaris error fails loudly.

## Explicit host apply

Do not run a host apply while either maintenance DAG is active. Review the plan,
then acknowledge irreversible snapshot expiration:

```bash
make iceberg-cdc-maintenance-apply
```

The command always runs stages in this order:

```text
compact_data_files -> expire_snapshots
```

It is safe to retry after a partial success. A completed compaction leaves no
eligible small-file group and converges to a no-op; expiration replans refs and
protected ancestors on every call.

For each stage, logs include bounded before/after file and snapshot counts,
bytes, candidate partitions/files, Trino procedure metrics, removed metadata
and data-file samples, observed concurrent `main` movement, raw-row
preservation, and event-ID uniqueness.

## Semantic safety and concurrent ingestion

The long-running CDC consumer is not paused. Before destructive work,
maintenance requires `count(*) = count(DISTINCT event_id)` and captures the
current Iceberg snapshot. After each stage it verifies:

1. every row and all 17 stored columns visible at the captured snapshot still
   exist unchanged in current state;
2. current rows did not decrease;
3. every current `event_id` is unique.

Rows appended concurrently are allowed and are reported as growth. Iceberg
optimistic commits arbitrate a concurrent append/rewrite conflict. A conflict or
Trino/Polaris outage fails the task; the bounded Airflow retry replans current
state. Maintenance never resets transport, edits Kafka offsets, or deletes CDC
rows as a recovery action.

## Scheduled operation

The `maintain_cdc_iceberg` DAG runs Sundays at `04:30 UTC`, with
`catchup=False` and `max_active_runs=1`:

```text
compact_data_files -> expire_snapshots
```

Both tasks use the one-slot `iceberg_bronze` pool. This serializes them with
Airflow-managed `load_bronze` and batch snapshot maintenance. It does **not**
lock the external CDC consumer or host-side commands. Compaction has a 60-minute
execution budget; expiration has 30 minutes. Both use two retries with
exponential backoff capped at 15 minutes.

New DAGs start paused. Validate policy values and one host preview before the
first unpause:

```bash
make airflow-up
docker compose exec airflow-scheduler airflow dags list | grep maintain_cdc_iceberg
docker compose exec airflow-scheduler airflow tasks states-for-dag-run \
  maintain_cdc_iceberg <dag-run-id>
```

## Verification

Run the preview again and inspect raw identity:

```sql
SELECT count(*) AS rows, count(DISTINCT event_id) AS distinct_event_ids
FROM iceberg.bronze.postgres_cdc_events;

SELECT "partition"."event_date", count(*) AS files,
       sum(record_count) AS rows, sum(file_size_in_bytes) AS bytes,
       min(file_size_in_bytes) AS minimum_file_bytes,
       max(file_size_in_bytes) AS maximum_file_bytes
FROM iceberg.bronze."postgres_cdc_events$files"
GROUP BY 1 ORDER BY 1;

SELECT committed_at, snapshot_id, parent_id, operation
FROM iceberg.bronze."postgres_cdc_events$snapshots"
ORDER BY committed_at DESC;

SELECT name, type, snapshot_id, min_snapshots_to_keep, max_snapshot_age_in_ms
FROM iceberg.bronze."postgres_cdc_events$refs"
ORDER BY name;
```

Expected results:

- row count still equals distinct event-ID count;
- file count falls materially when a partition has many small files;
- a repeat apply reports zero rewritten/added files when no new group exists;
- `main`, non-main refs, snapshots younger than retention, and at least the
  configured recent `main` ancestors remain;
- a retained snapshot remains readable with `FOR VERSION AS OF`;
- consumer lag and committed offsets remain healthy;
- the CDC-selected dbt graph produces the same typed/delete-aware state.

The measured 210,000-row result and exact reproduction steps are in the
[CDC maintenance benchmark](../benchmarks/cdc-maintenance.md).

## Failure and retry

The command/task fails for unsafe policy, malformed procedure metrics,
duplicate event IDs, missing/changed old rows, a protected snapshot/ref loss,
commit conflict, timeout, or Trino/Polaris failure. It never converts these to a
successful warning.

1. Preserve the CLI or Airflow task log, including pre-run snapshot ID and file
   counts.
2. Check core and streaming health:

   ```bash
   docker compose --profile core ps
   make streaming-status
   docker compose logs --tail=200 trino polaris cdc-consumer
   ```

3. If the consumer is catching up, allow it to reach lag zero for easier
   reconciliation; do not reset it.
4. Run the read-only maintenance plan again.
5. Retry. A finished compaction or expiration stage converges safely.
6. After recovery, verify row identity, lag, and the selected dbt graph.

Do not lower safety floors, run `remove_orphan_files`, delete MinIO objects,
change consumer batch size, or reset Kafka/Bronze to clear a maintenance error.

## Recovery limits

Expired CDC snapshots cannot be restored in place. The current table remains
protected, but rollback older than the retained history is limited:

- Kafka data topics retain only seven days or 5 GiB per partition;
- Debezium/consumer offsets describe transport progress, not an archive of
  expired Iceberg files;
- a transport reset creates new Kafka coordinates and new event IDs;
- there is no separate immutable object archive for the CDC ledger.

If a semantic postcondition fails, stop maintenance and downstream publication,
preserve all available snapshots/logs, and reconcile against PostgreSQL and
Kafka before taking any destructive action. Do not claim archive rebuildability
or exactly-once recovery.
