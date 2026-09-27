# ADR 0004 — ClickHouse serving layer: deployment and Gold publication mechanism

## Title

Introduce ClickHouse in the `bi` Compose profile with versioned migrations and a Python publisher that copies Gold marts from Iceberg into ClickHouse via an atomic staging-table swap (`EXCHANGE TABLES`), keeping Iceberg the source of truth and BI strictly read-only.

## Status

Accepted.

## Context

Phase 6 (`ROADMAP.md`) adds the low-latency serving layer: publish Gold marts
from Iceberg to ClickHouse, make publication idempotent, keep ClickHouse fully
rebuildable from Iceberg Gold, and give BI a read-only account. The serving
rules (`docs/agent/serving-bi.md` §26) require every serving dataset to be
reconstructable from Gold, publication to be repeatable/idempotent/recoverable/
measurable, intentional table design, and no write permissions for BI.

ClickHouse itself is already an accepted platform technology (AGENTS.md §3.1),
so this ADR is not about *whether* to use it, but about the deployment model
and the publication mechanism — a new persistence/feeding pattern (AGENTS.md
§42). Facts that constrain the decision:

- Gold marts already exist in Trino as `iceberg.analytics.mart_daily_sales`,
  `mart_customer_ltv`, `mart_marketing_roi`, `mart_delivery_performance`.
  Three of the four are full-history snapshot aggregates by nature (LTV,
  marketing ROI, delivery performance); only `mart_daily_sales` is naturally
  date-partitioned. A funnel mart does not exist yet — its clickstream source
  arrives in Phase 9, so it is deferred.
- Data volumes are workstation-scale (OLTP generator at 10k/5k/100k rows);
  a full mart snapshot is at most a few hundred thousand rows.
- Host port `127.0.0.1:9000` is already taken by MinIO, so ClickHouse's
  native port 9000 must stay Docker-network-internal.
- The Bronze loader (`src/omni_retail/lakehouse/bronze/`) already talks to
  Trino through the `trino` Python client — the serving publisher can reuse
  the same access path and module shape.
- Orchestration (Phase 5 pattern) expects runnable code to live in the
  `omni_retail` package, with DAGs as thin wrappers (AGENTS.md §19).

## Decision

### Deployment model

- **Compose profile `bi`** (ROADMAP §4): `clickhouse` service with a pinned
  `clickhouse/clickhouse-server:<exact-release>` image (no `latest`), named
  volume `clickhouse-data`, healthcheck (`SELECT 1` via the in-image client),
  and **loopback-only HTTP port `127.0.0.1:8123`** for host-side tooling and
  tests. The native port 9000 is not published to the host (collision with
  MinIO); in-network consumers use `clickhouse:8123`/`clickhouse:9000`.
  Superset joins this profile in Phase 7 — nothing Superset-specific is built
  now.
- **Versioned migrations** under `clickhouse/migrations/*.sql`, applied by a
  one-shot `clickhouse-init` container (mirrors `minio-init`/`polaris-init`):
  runs pending files in lexical order and records them in a
  `schema_migrations` ledger table. No console-only schema changes
  (AGENTS.md §45).
- **Service accounts** (least privilege, secrets only in `.env`):
  - `default` (admin) — used solely by `clickhouse-init` for migrations;
  - `omni_publisher` — full grants on `analytics.*` only; used by the
    publisher;
  - `superset_reader` — `SELECT` on `analytics.*` only; used by BI
    (Phase 7).

### Publication mechanism

- **Publisher module** `src/omni_retail/serving/clickhouse/` with the same
  shape as the Bronze loader: env-driven typed config, a mart registry
  (`specs.py`), a `cli.py` (`publish --mart ...`, `rebuild --mart ...`), and
  structured logging (mart name, rows written from the insert summary,
  duration). Host CLI runs are for development; the Airflow DAG in Phase 6
  slice 3 calls the same functions in-process (ADR 0003 model).
- **Data path**: read a full mart snapshot from Trino
  (`SELECT ... FROM iceberg.analytics.<mart>`) via the `trino` client, insert
  into ClickHouse through `clickhouse-connect` (official HTTP driver, also the
  Superset-recommended one) into a `<mart>_staging` table, then atomically
  swap: `EXCHANGE TABLES <mart>_staging AND <mart>`.
- **Publish cycle**: `TRUNCATE <mart>_staging` → insert snapshot →
  `EXCHANGE TABLES`. The serving table is never written to directly; BI never
  observes a partial publish; a failure at any step leaves the previous
  serving data intact and recovery is a plain rerun (§26.3 recoverable).
- **Idempotency and rebuild share one code path**: re-publishing over an
  identical Gold state produces an identical serving table (no duplicates, no
  watermarks, no dedup engine); rebuilding after `DROP` is
  migrations-recreate + the same publish. This is the simplest mechanism that
  satisfies the acceptance criteria (AGENTS.md §53).
- **Publication mode** (documented per §26.3): **full snapshot swap** for all
  four marts. Incremental `ALTER TABLE ... REPLACE PARTITION` remains an
  explicitly reserved option for `mart_daily_sales` only (the single
  date-partitioned mart), to be evaluated in slice 3 when Airflow wiring
  exists; the full swap stays the fallback. Staging tables mirror the serving
  DDL (engine, `ORDER BY`, partition key) so the partition-replace option
  stays available without migration rework.
- **Engine choices (`ORDER BY`, partitioning, TTL)** are intentionally
  deferred to slice 2 and will be documented in `docs/data-model.md` with
  per-mart rationale (guide §26.2: intentional, not default).

## Alternatives considered

- **ClickHouse `iceberg()`/`icebergS3()` table functions reading MinIO
  directly:** rejected — the function is path/metadata-JSON based (the caller
  resolves table locations instead of a catalog), read-only for querying with
  experimental write flags, and catalog integration is Glue-oriented with no
  first-class Polaris REST catalog support. It would also duplicate S3
  credentials into ClickHouse and bypass Trino as the single lakehouse
  compute entry, without by itself solving idempotent publication.
- **dbt with a `dbt-clickhouse` adapter (second dbt target):** rejected —
  publication is a copy of already-modeled Gold data, not a transformation;
  a second adapter/project would blur the "Iceberg Gold is the source of
  truth, ClickHouse is derived" boundary and add dependency surface.
- **INSERT-append publication:** rejected — not idempotent; violates the
  Phase 6 acceptance criteria and would force watermark bookkeeping plus a
  dedup strategy after the fact.
- **`ReplacingMergeTree` upserts keyed by version:** rejected — asynchronous
  merges make "rebuild == republish" reasoning subtle (queries need
  `FINAL`/`argMax` or post-merge waits) and the complexity buys nothing while
  the atomic swap already guarantees idempotency.
- **Materialized views computing marts inside ClickHouse:** rejected —
  business logic would exist only in ClickHouse, violating AGENTS.md §3.2.
- **Incremental `REPLACE PARTITION` for all marts:** deferred, not rejected —
  only `mart_daily_sales` is naturally date-partitioned; the other three are
  full-history snapshots where "incremental" would mean "rewrite almost
  everything anyway". Revisit per-mart in slice 3.

## Consequences

- Positive:
  - idempotency, recoverability, and rebuildability are structural properties
    of the swap, not tested-in afterthoughts; a single publish path covers
    routine refresh, rerun after failure, and full rebuild;
  - Trino remains the only engine with lakehouse business semantics;
    ClickHouse holds derived copies only (AGENTS.md §3.2);
  - BI sees either the previous or the new snapshot, never a partial one.
- Negative / accepted costs:
  - full-snapshot publication cost grows with mart size — acceptable at
    workstation scale, with the incremental option reserved for
    `mart_daily_sales`;
  - staging twins roughly double serving storage (small in absolute terms);
  - `EXCHANGE TABLES` requires the Atomic database engine (the default) —
    non-atomic database engines are ruled out;
  - a Gold mart schema change needs a paired migration (serving + staging)
    and a republish — enforced by the migration ledger;
  - new pinned dependency `clickhouse-connect` in `uv.lock`; from slice 3,
    publisher changes require rebuilding the custom Airflow image (ADR 0003
    consequence).
- Measurability: per-publish logs (rows written, bytes, duration) from day
  one; the Trino-vs-ClickHouse benchmark and orchestration visibility land in
  slice 3 as planned.

## Rollback / migration considerations

- Removing the serving layer entirely: drop the `bi` profile, `clickhouse/`,
  `src/omni_retail/serving/`, env vars, and the Makefile targets; no lakehouse
  data depends on ClickHouse.
- Switching `mart_daily_sales` to incremental `REPLACE PARTITION` in slice 3:
  additive — partition the serving/staging pair via a new migration,
  republish once; the swap path remains as fallback.
- Changing engine choices in slice 2: new migration (drop/recreate the
  serving + staging pair) plus republish — data is derived, so no backfill of
  source data is involved.
- Replacing the publisher runtime later (e.g., a Spark job for large backfills
  in Phase 9+): the mart registry and CLI/DAG surface stay; only the
  implementation behind them changes.
