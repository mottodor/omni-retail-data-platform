# Active plan — Phase 6: ClickHouse serving layer (sliced)

Task: implement Phase 6 of `ROADMAP.md` as three vertical slices. This plan
covers the slice breakdown and the detailed checklist for slice 1; slices 2–3
are outlined and get their own detailed checklists when reached. Division of
labor (AGENTS §41.3): acceptance criteria live in `ROADMAP.md`, architectural
decisions in `docs/adr/`, status in `PROGRESS.md`; this file owns execution
detail. Delete this file in the closing commit of the phase.

Created: 2026-09-25 (planning session).

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

- [ ] 2.1 Migrations for `mart_customer_ltv`, `mart_marketing_roi`,
  `mart_delivery_performance` (+ staging twins).
- [ ] 2.2 Engine decisions per mart (MergeTree ORDER BY / partitioning / TTL
  rationale) — documented in migration comments + `docs/data-model.md`.
- [ ] 2.3 Publisher specs extended to all marts; reconciliation tests
  (checksums Gold vs CH per mart).
- [ ] 2.4 `serving-rebuild` target: drop + republish every mart from Iceberg
  (the "rebuildable from Gold" acceptance criterion, one command).
- [ ] 2.5 PROGRESS.md: slice status; record funnel-mart deferral.

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
