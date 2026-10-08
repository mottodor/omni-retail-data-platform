# Bronze full rebuild and recovery

Use this runbook only when the Bronze Iceberg layer must be rebuilt from the
existing immutable `archive` bucket. It is explicitly destructive for Bronze,
but does **not** modify raw archive objects, PostgreSQL, Silver, Gold,
ClickHouse, or Superset.

## Preconditions

- The core stack is healthy: `make up` and `make smoke-core` pass.
- `.env` exists and `ICEBERG_BRONZE_SCHEMA` names `bronze` (or an explicitly
  disposable `*_bronze` schema).
- The raw archive contains complete manifests for every source/date to recover.

## Run

```bash
make bronze-rebuild
```

The command drops only `iceberg.${ICEBERG_BRONZE_SCHEMA:-bronze}`, then uses
Bronze `run-new` to rebuild all fourteen batch tables from archive. It does not
clear any source or serving state. PostgreSQL/API tables resume by date;
supplier-file tables scan canonical completed manifests so late files on older
dates are included. Logs identify source, logical date, object batches, chunk,
and retry context.

Trino 483 is affected by the REST OAuth2 session defect fixed upstream in
`trinodb/trino#30816`. The repository's pinned local Trino image backports that
exact merged change: `session=NONE` keeps service-principal semantics while
reusing the catalog OAuth2 session instead of fetching a token for every
operation. The rebuild also caps user query memory at 1 GiB and total query
memory at 1.5 GiB, leaving heap for Iceberg/REST-catalog allocations; each
INSERT remains below the configured 2,000,000-character query-text limit. The
larger bounded batches reduce Iceberg commits and REST-catalog metadata growth
within the workstation's 4 GiB Trino heap.

The loader retries a transient catalog failure per chunk. If that finite retry
is exhausted, the wrapper restarts Polaris, waits for `/q/health` (120 seconds
by default), and resumes `run-new` without dropping Bronze again. A failed
Trino connection — including a connection reset after its OOM exit — is also
recoverable: the wrapper restarts Trino, waits until `SELECT 1` succeeds, then
resumes from the stored chunk coordinates. Both recovery budgets default to two
restarts; set `BRONZE_REBUILD_MAX_POLARIS_RESTARTS`,
`BRONZE_REBUILD_MAX_TRINO_RESTARTS`, or
`BRONZE_REBUILD_HEALTH_TIMEOUT_SECONDS` for one run. Credentials are never
logged.

Exit code `75` means the Polaris catalog recovery budget was exhausted; `76`
means the Trino recovery budget was exhausted. Any other non-zero exit is a
data, schema, manifest, or unsupported infrastructure failure and deliberately
does not restart either service.

## Verify

```bash
make bronze-load ARGS="run-new"   # should be an idempotent no-op
make dbt-build
make serving-rebuild
```

For a targeted table/date check, compare the manifest row count to Bronze and
check that each raw coordinate is unique:

```sql
SELECT count(*), count(DISTINCT ROW(_source_object, _source_object_row_position))
FROM iceberg.bronze.orders
WHERE _batch_date = DATE '2026-09-18';
```

For PostgreSQL/API tables, the two values must equal the archived manifest's
`row_count`. For supplier-file tables, they must equal the sum of
`row_count - rejected_row_count` across completed manifests for that source and
date. Also group by `_batch_id, _source_object` to verify that every file row is
owned by its content-addressed manifest; rejected manifests authorize no rows.

## Manual recovery

If the wrapper exits after either bounded restart budget, preserve its logs and
inspect `docker compose --profile core logs --tail=200 trino polaris`. Resolve
the infrastructure failure, verify `make smoke-core`, then rerun
`make bronze-rebuild`; it is safe to start again because the command clears
only the configured Bronze schema. Do not bypass the pinned backport with
static tokens, `session=USER`, or Polaris token-TTL changes: those alter the
authentication contract instead of repairing the known `session=NONE` defect.

Replace the local backport with an unmodified Trino image only in a separate
compatibility task after a release containing PR 30816 is available.
