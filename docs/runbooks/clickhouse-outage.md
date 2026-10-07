# ClickHouse outage and republish

ClickHouse is a derived serving layer. Iceberg Gold remains the source of
truth, so recovery republishes the affected mart(s) rather than restoring
business data from ClickHouse.

## Detect and assess

1. Check the ClickHouse health endpoint:

   ```bash
   curl --fail http://127.0.0.1:8123/ping
   docker compose --profile bi ps clickhouse clickhouse-init
   ```

2. Check recent logs without printing environment secrets:

   ```bash
   docker compose --profile bi logs --tail=100 clickhouse
   ```

3. If ClickHouse is unavailable, Superset queries fail, but the Iceberg Gold
   datasets and upstream transformation pipeline remain available. Pause or
   leave `publish_serving` paused until the service is healthy.

## Restore the service

The BI profile is intentionally separate from the core stack:

```bash
make up
make bi-up
```

`clickhouse-init` applies versioned migrations idempotently. If a stopped
one-shot init container has a stale local bind mount, remove only that
container and retry:

```bash
docker compose --profile bi rm -f clickhouse-init
make bi-up
```

Do not run `make reset` as routine outage recovery: it destroys core named
volumes and is not needed to rebuild a derived serving table.

## Republish

Republish all four available marts from the current Iceberg Gold snapshot:

```bash
make serving-rebuild
```

Republish one affected mart when the failure is isolated:

```bash
make serving-rebuild ARGS="--mart mart_daily_sales"
# or: mart_customer_ltv, mart_marketing_roi, mart_delivery_performance
```

Publication loads a staging table and atomically exchanges it with the serving
table. It is safe to retry. The CLI fails fast; already completed marts remain
consistent and can be rerun safely.

If Gold is stale, rebuild it first and then publish:

```bash
make dbt-build
make serving-rebuild
```

## Validate recovery

Run the serving integration checks when the core and BI profiles are healthy
and the normal Gold snapshot is populated:

```bash
make dbt-build       # when Gold is missing or stale
make serving-rebuild
make integration
```

If ClickHouse is unreachable, its optional integration module is skipped with
`make bi-up` as the recovery action. Missing or empty normal Gold/serving marts
also produce an actionable prerequisite skip. Once ClickHouse and Gold are
present, unexpected Trino/catalog errors, publication failures, permissions
regressions, and reconciliation differences fail loudly.

The executed checks compare row counts and ordered row values between Trino
Gold and ClickHouse for every mart and assert that `superset_reader` INSERTs
are rejected, so the read-only boundary is re-verified as part of recovery.
Credentials stay in `.env`; never copy them into tickets or logs.

Finally, verify that the `transform_lakehouse` DAG can run its
`publish_serving` task after `dbt_build`; publication is part of that same
stable-boundary workflow. The DAG is paused by default in a new environment;
unpause it only after validating the profiles, credentials, and CDC bootstrap
gate in the Kafka runbook.

## Known limitations

- The current publication is a full snapshot, not incremental partition
  replacement.
- A failed all-mart rebuild may leave earlier marts refreshed and later marts
  unchanged; rerun `make serving-rebuild` to converge the complete layer.
- Superset dashboard rendering is a manual post-recovery check; automated
  serving tests validate data and permissions but do not exercise a browser.
