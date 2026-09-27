# Active plan — Phase 6: ClickHouse serving layer (sliced)

Task: implement Phase 6 of `ROADMAP.md` as three vertical slices. This plan
owns the slice breakdown, the detailed checklists for slices 1–2, and the
outline of slice 3 (which gets its own detailed checklist when reached).
Division of labor (AGENTS §41.3): acceptance criteria live in `ROADMAP.md`,
architectural decisions in `docs/adr/`, status in `PROGRESS.md`; this file
owns execution detail. Delete this file in the closing commit of the phase.

Created: 2026-09-25 (planning session).
Updated: 2026-09-27 — slice 2 detailed checklist (planning session).

## Goal

Publish Gold marts from Iceberg to ClickHouse as a rebuildable, idempotent,
read-only-for-BI serving layer:

- Iceberg stays the source of truth (AGENTS §3.2, guide §26);
- every ClickHouse dataset is reconstructable from Iceberg Gold (guide §26.1);
- publication is repeatable / idempotent / recoverable / measurable (§26.3);
- `superset_reader` has SELECT-only access (§26.4, AGENTS §10).

## Context to read first (session resume)

1. `AGENTS.md` core + `docs/agent/serving-bi.md` (§25–27) +
   `docs/agent/engineering-practices.md` (§40–45).
2. `ROADMAP.md` — Phase 6 section + §4 (profiles) + §5 (repo structure).
3. `PROGRESS.md` — current focus and deferred items.
4. `docs/adr/0001-project-architecture.md`, `docs/adr/0003-airflow-deployment.md`
   (image/deployment conventions reused for slice 3).
5. Implementation neighbors to mirror:
   - `src/omni_retail/lakehouse/bronze/` (loader, specs, cli, `trino` client
     usage — the publisher follows the same shape);
   - `infrastructure/scripts/minio_init.sh` / `polaris_init.py` (one-shot init
     service pattern);
   - `tests/integration/` (live-stack test conventions).

## Facts established at planning time

- Gold objects already exist: `iceberg.analytics.mart_daily_sales`,
   `mart_customer_ltv`, `mart_marketing_roi`, `mart_delivery_performance`
   (dbt `marts` → `analytics` schema; facts/dims in `gold`).
- **No funnel mart exists** — funnel needs clickstream (visit / product_view /
  cart), which arrives in Phase 9. Decision: Phase 6 publishes the 4 existing
  marts; the funnel mart is deferred until its source data exists (recorded in
  `PROGRESS.md` deferred list at slice-2 completion). ROADMAP Phase 6 wording
  "marts: sales, funnel, LTV, marketing, delivery" is satisfied for the 4
  available ones.
- Host port `127.0.0.1:9000` is taken by MinIO; ClickHouse native port 9000
  stays docker-network-only. Expose only HTTP `127.0.0.1:8123` for host-side
  CLI/tests (clickhouse-connect uses HTTP).
- Compose profile: `bi` (ROADMAP §4). Superset joins the same profile in
  Phase 7 — do not create it now.
- Driver: `clickhouse-connect` (official, HTTP; also the Superset-recommended
  driver), pinned via uv. `trino` client (already a dependency) reads Gold.
- Repo layout per ROADMAP §5: migrations/tests under top-level `clickhouse/`,
  Python code under `src/omni_retail/serving/clickhouse/`.

## Decision to ratify first — ADR 0004 (publication mechanism)

Recommended (to be confirmed/adjusted when writing the ADR as step 1.1):

- **Publisher**: Python module `src/omni_retail/serving/clickhouse/` —
  reads a full mart snapshot via Trino (`SELECT` from `iceberg.analytics.*`),
  inserts into a `_staging` table, then atomically swaps it with the serving
  table (`EXCHANGE TABLES`).
- **Idempotency model**: per-mart full-snapshot atomic swap. Re-running any
  publish never duplicates rows; a rebuild is the same operation after DROP.
  This satisfies §26.3 with the simplest correct mechanism (AGENTS §53).
- **Incremental option**: `ALTER TABLE ... REPLACE PARTITION` for
  `mart_daily_sales` (the only naturally date-partitioned mart) — evaluate in
  slice 3 when wiring Airflow; full swap stays the fallback.
- **Alternatives for the ADR**: ClickHouse `iceberg()`/`s3()` table functions
  (bypasses Trino, duplicates catalog/S3 config, weaker type fidelity);
  Airflow operator (no first-class operator; code must live in the baked image
  anyway); ReplacingMergeTree upserts (merge semantics complicate "rebuild ==
  republish" reasoning); INSERT-only append (not idempotent).
- **Engines (ratified in slice 2, documented then)**: MergeTree for all marts
  (swap-based publication removes the need for dedup engines); `ORDER BY` per
  BI access pattern; partitioning only where justified (candidate: monthly for
  `mart_daily_sales`); TTL not justified — historical portfolio data, document
  the rationale.

---

## Steps

Vertical slices; each closes with its own validation and PROGRESS update.

### Slice 1 — ClickHouse vertical slice (one mart end-to-end)

Scope: infra + users + migrations + publisher for `mart_daily_sales` only,
with idempotency and rebuild proven by tests.

- [x] 1.1 Write and accept `docs/adr/0004-clickhouse-serving-publication.md`
  (accepted 2026-09-25: full-snapshot staging swap via `EXCHANGE TABLES`;
  engines/ORDER BY deferred to slice 2 with a pointer; incremental
  `REPLACE PARTITION` reserved for `mart_daily_sales`, eval in slice 3).
- [x] 1.2 Compose (`docker-compose.yml`, profile `bi`):
  - `clickhouse` service: pinned `clickhouse/clickhouse-server:26.8.11.7`
    (latest LTS patch), loopback-only `127.0.0.1:8123`, healthcheck
    (`clickhouse-client SELECT 1` or HTTP ping), named volume
    `clickhouse-data`, secrets from `.env`;
  - `clickhouse-init` one-shot: applies `clickhouse/migrations/*.sql` in order,
    tracks applied files in a `schema_migrations` ledger table (AGENTS §45 —
    no console-only changes), restart: "no", depends_on healthy.
- [x] 1.3 `clickhouse/migrations/`:
  - `0001_analytics_database.sql` — `analytics` DB;
  - `0002_users.sql` — `default` (password from env) and `superset_reader`
    (SELECT-only on `analytics.*`); also `omni_publisher` per ADR 0004;
  - `0003_mart_daily_sales.sql` — serving table + matching `_staging` table
    (identical engine/ORDER BY/partition, required for the swap). Money
    columns mirror Gold types exactly (`Decimal(38,21)`/`Decimal(38,6)`,
    Trino division artifacts).
- [x] 1.4 `.env.example`: `CLICKHOUSE_HOST/PORT/HTTP_PORT`, admin and
  `superset_reader` credentials placeholders.
- [x] 1.5 `src/omni_retail/serving/clickhouse/`:
  - `config.py` (env-driven, typed), `client.py` (clickhouse-connect wrapper),
  - `publisher.py` (snapshot read via Trino → staging insert → `EXCHANGE
    TABLES`; structured logging with rows written from QuerySummary;
  - `specs.py` (mart registry: source table, target, columns), `cli.py` +
    `__main__.py` (`publish --mart mart_daily_sales`, `rebuild --mart ...`).
- [x] 1.6 Tests:
  - unit (`tests/unit/serving/`): spec/SQL generation, swap statement
    sequencing, config parsing — fakes only, no network (AGENTS §48);
  - integration (`tests/integration/test_serving_publication.py`, requires
    core + bi profiles): (a) publish twice → identical row count + checksum;
    (b) drop mart → rebuild → identical; (c) `superset_reader` INSERT fails;
    (d) Trino-side row count == CH row count.
- [x] 1.7 Makefile: `bi-up` / `bi-down` (profile), `serving-publish`
  (ARGS="--mart mart_daily_sales"), include CH integration in `integration`.
- [x] 1.8 Docs: README (new profile in run instructions),
  `docs/data-model.md` (serving layer section), `PROGRESS.md` (phase status →
  in progress, slice table).
- [x] 1.9 Validation (see commands below) + completion report (AGENTS §49).
  Notes: image pinned to the LTS line 26.8 (26.8.11.7, four-part release);
  `tests/test_compose_guards.py` one-shot set gained `clickhouse-init`;
  active-plan headings realigned with the §41.3 contract guard (broken by
  the uncommitted planning-session rewrite); serving tests skip with an
  actionable reason when the bi profile or Gold marts are absent.

### Slice 2 — remaining marts + engine design

Scope: publish the remaining three Gold marts, ratify and document the
per-mart physical design (engine / ORDER BY / partitioning / TTL), prove
Gold-vs-CH reconciliation for every mart, and make the whole serving layer
rebuildable with one command.

Facts established at planning time (2026-09-27):

- Existing Gold marts and grains (dbt `marts`, confirmed against model SQL;
  exact Trino types must be confirmed against the live schema when writing
  the specs — `SHOW COLUMNS FROM iceberg.analytics.<mart>` — and mirrored
  1:1 as slice 1 did):
  - `mart_customer_ltv` — 1 row per customer: `customer_id`,
    `customer_key`, `region`, `segment`, `first_order_date`,
    `last_order_date`, `orders_count`, `gmv_eur`, `avg_order_value_eur`
    (division → nullable decimal); no natural filter date → no
    partitioning.
  - `mart_marketing_roi` — 1 row per campaign: `campaign_id`,
    `campaign_name`, `channel`, `campaign_status`, `start_date`,
    `end_date`, `budget_eur`, `spend_eur`, `impressions`, `clicks`, `ctr`,
    `cpc_eur`, `cpm_eur`, `budget_utilization` (doubles); no partitioning.
  - `mart_delivery_performance` — 1 row per carrier: `carrier`,
    `delivery_count`, `delivered_count`, `in_transit_count`,
    `delayed_count`, `returned_count`, `avg_transit_hours`,
    `last_update_at` (timestamp); no partitioning.
- Physical design (ratifies the ADR 0004 deferral, guide §26.2):
  - MergeTree for all four marts — swap-based publication removes any need
    for dedup engines.
  - `ORDER BY` per BI access pattern: LTV top customers per segment/region;
    campaigns by channel and date; deliveries by carrier.
  - Partitioning only for `mart_daily_sales`: monthly
    `PARTITION BY toYYYYMM(order_date)`. Functional justification: keeps
    the slice-3 `REPLACE PARTITION` option open; the other three marts are
    small dimension-like tables — unpartitioned.
  - TTL not justified — historical portfolio data; document the rationale.
- Money scales: keep mirroring the Gold types exactly
  (`Decimal(38,21)` / `Decimal(38,6)` division artifacts included) — the
  exact round-trip is what makes reconciliation trivially provable. The
  "normalization is a slice 2 call" notes in migration 0003 and specs.py
  resolve to "keep the mirror"; document in `docs/data-model.md`.
- Reconciliation: per mart, row count + full ordered row-by-row comparison
  of the Trino snapshot vs the CH table (fetched via the two clients,
  values normalized for comparison — dates, decimals, floats). Portfolio
  scale makes this exact and deterministic; cross-engine SQL checksums are
  fragile (different hash/rounding semantics) and are not used.
- `mart_daily_sales` physical change lands as an append-only migration that
  drops and recreates the table pair with the partition clause — serving
  data is derived, republish afterwards, no data migration.

- [x] 2.1 `specs.py` — `MartSpec`: add optional `partition_by` (expression
  string, e.g. `toYYYYMM(order_date)`) with `PARTITION BY` emitted by
  `create_table_sql`; unpartitioned marts emit no clause (slice-1 DDL for
  the other tables stays byte-identical).
  Note: exact Trino types confirmed against the live schema
  (`SHOW COLUMNS FROM iceberg.analytics.<mart>`), incl. the empirical NULL
  check that fixed the Nullability set (avg_order_value_eur is NULL for
  real rows; DateTime64(6, 'UTC') for timestamptz).
- [x] 2.2 Migrations `0004_mart_customer_ltv.sql`,
  `0005_mart_marketing_roi.sql`, `0006_mart_delivery_performance.sql` —
  serving + `_staging` twins each, engine/ORDER BY rationale in comments.
- [x] 2.3 Migration `0007_mart_daily_sales_monthly_partition.sql` — drop +
  recreate both `mart_daily_sales` tables with
  `PARTITION BY toYYYYMM(order_date)`; update the `MART_DAILY_SALES` spec;
  note the required republish in the migration comment.
- [x] 2.4 `specs.py`: add the three new specs (exact Trino types confirmed
  live in 2.1–2.2); `MARTS` registry holds all 4 marts; money-scale
  decision documented next to the specs.
  Note: decision = keep the exact mirror (round-trip 1:1 makes
  reconciliation provable); documented in specs.py + data-model.md.
- [x] 2.5 Unit tests (`tests/unit/serving/clickhouse/`): registry holds 4
  marts; specs↔migrations cross-check extended to every mart including the
  partition clause; DDL generation for partitioned vs plain tables.
  Note: cross-check reads the CURRENT-DDL carrier per mart (0007 for
  mart_daily_sales; 0003 stays in the ledger as history); also added a
  Nullability-set test and a registry-order test.
- [x] 2.6 Integration tests (`tests/integration/test_serving_publication.py`):
  publish all four marts; per-mart Gold-vs-CH reconciliation (row count +
  ordered row-wise equality); republish idempotency for one new mart
  (pattern proven in slice 1); teardown → rebuild → reconcile everything.
  Note: reconciliation compares Python-side sorted multisets (normalized
  values; tz-aware Trino timestamptz vs naive-UTC CH DateTime64) — no
  cross-engine SQL checksums; full rebuild goes through the real
  `cli.main(["rebuild", "--all"])` entrypoint.
- [x] 2.7 CLI + Makefile: `rebuild --all` (publish stays per-mart);
  `make serving-rebuild` without ARGS rebuilds every mart; help text.
  Note: fail-fast semantics documented in `run_command`; the Makefile
  passes `--all` explicitly when ARGS is empty (no implicit CLI default);
  both serving Makefile targets now set `no_proxy=127.0.0.1,localhost`
  like the integration target (wildcard `127.*` in the ambient no_proxy
  is not understood by Python HTTP clients → HTTP 403 via proxy).
- [x] 2.8 Docs: `docs/data-model.md` — per-mart physical design table
  (engine / ORDER BY / partitioning / TTL rationale) + money-scale note;
  README serving-rebuild usage if it documents ARGS; `PROGRESS.md` slice 2
  status + funnel-mart deferral entry in the deferred list.
- [x] 2.9 Validation: `make lint`, `make test`, `docker compose config`,
  `make up && make bi-up` (0004–0007 apply cleanly on the existing ledger),
  `make integration` (CH tests incl. reconciliation), completion report
  (AGENTS §49).
  Note: all executed and passing; bi profile was already up —
  `clickhouse-init` re-run applied 0004–0007 (applied=4 skipped=3); one
  env hiccup: the stopped init container had a stale WSL bind mount and
  needed `docker compose --profile bi rm -f clickhouse-init` before
  `make bi-up` (Docker Desktop quirk, not a repo defect).

### Slice 3 — orchestration + benchmark

- [ ] 3.1 Airflow DAG `publish_serving`, dataset-triggered by
  `transform_lakehouse` output (same pattern as Phase 5 slice 3); publisher
  code baked into the custom image → `make airflow-build` required
  (ADR 0003 consequence); DAG + structure tests in `airflow/tests/`.
- [ ] 3.2 Benchmark: identical BI query via Trino→Iceberg vs ClickHouse;
  capture latency (repeated runs, cold/warm) + `EXPLAIN`/`EXPLAIN ANALYZE`
  output; report under `docs/benchmarks/`.
- [ ] 3.3 Runbook: ClickHouse outage / republish procedure
  (`docs/runbooks/`).
- [ ] 3.4 Close phase: PROGRESS.md (Phase 6 done), delete this plan file.

## Validation commands

Per slice, run what applies (AGENTS §5.3 — only claim what was executed):

```bash
make lint                        # ruff + mypy
make test                        # unit tests (fakes, no stack)
docker compose config            # compose validity incl. bi profile
make up && make bi-up            # core + ClickHouse healthy
make integration                 # incl. CH publication tests (slices 1–2)
make dbt-build                   # marts fresh before publish (if needed)
make airflow-build && make airflow-test   # slice 3 only
```

## Scope fence (do NOT)

- No Superset — Phase 7 (do not create `superset/` or dashboards).
- No funnel mart — deferred to Phase 9 (no clickstream source yet).
- No Gold/silver/bronze schema or dbt model changes; ClickHouse is derived.
- No business logic living only in ClickHouse (AGENTS §3.2) — serving tables
  mirror Gold marts 1:1.
- No Kafka/Spark/observability work (later phases, AGENTS §4 gates).
- No host exposure of ClickHouse beyond loopback 8123; no write grants for BI.
- No new platform technology beyond ClickHouse (already accepted, §3.1).
