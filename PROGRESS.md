# PROGRESS.md — current project state

Single home of **current state**: what is done, what is in flight, and what is
deliberately deferred. Plans and acceptance criteria live in `ROADMAP.md`;
architectural decisions live in `docs/adr/`; the audit trail of *when*
things happened is git history — this file only indexes *now*.

Contract:

- status only — no narratives, no plan copies; keep under ~100 lines;
- updated in the same PR/commit as the work it reflects (AGENTS.md §6, DoD);
- read at session start (AGENTS.md §5.1); updated whenever a task changes
  phase/slice status, the current focus, or the deferred list (AGENTS.md §49);
- no manually maintained "last updated" date: the date of any change is the
  commit date (`git log -1 --date=short -- PROGRESS.md`).

## Current focus

Platform follow-up #16 is complete: every host-side Makefile target uses one
shared localhost proxy bypass while preserving caller exclusions; live
acceptance passed with deliberately unusable proxy URLs. Phase 8 (CDC:
Debezium -> Kafka -> Iceberg) is next.

Reliability fix #15 is complete: the full archive-to-Bronze rebuild now uses
bounded query memory/text, fewer commits, and finite automatic Polaris/Trino
recovery; its live acceptance run completed from the seeded archive with two
Trino restarts and no operator intervention. Reliability debt #14 is complete:
integration tests restore exact object mutations,
enforce a full-archive checksum invariant, and use disposable lakehouse
schemas; two consecutive live runs preserved all production table snapshots
and left no `it_*` schemas. Phase 7 (Apache Superset) is **done** and delivered end
to end: custom pinned image
`omni-retail/superset:0.1.0`, `bi`-profile services with dedicated metadata
PostgreSQL, idempotent bootstrap (ADR 0005) with fixed connection UUIDs, and
BI-as-code — the four mart datasets plus the Sales, Executive, Customer and
Marketing dashboards committed as sanitized import/export v1 bundles under
`superset/assets/` and re-imported on every `make bi-up` (verified by a
metadata-volume-wipe round-trip). Dashboard URLs are slug-based. The Trino
SQL Lab ad-hoc path (Superset -> Trino -> Iceberg) is verified by an
integration test. Remaining manual step: dashboard screenshots for the
README gallery (procedure in `docs/screenshots/README.md`; no browser in
the agent environment). Funnel dashboard and conversion/ROAS metrics are
deferred to Phase 9 (no clickstream/attribution data yet).

Phase 6 delivered the `bi` profile and migrations ledger, least-privilege
`omni_publisher`/`superset_reader` accounts, idempotent staging-swap
publication for all four available marts, per-mart physical design,
Gold-vs-ClickHouse reconciliation, one-command rebuild, dataset-triggered
Airflow publication, Trino-vs-ClickHouse benchmark, and the ClickHouse
outage runbook.

Phase 5 delivered end to end: Bronze loader (Iceberg via Polaris), Silver/Gold
Kimball model with SCD2 `dim_customer`, `int_orders_fx` + four EUR-normalized
analytics marts, dataset-triggered `load_bronze` -> `transform_lakehouse`
orchestration (+ DAG structure tests), and a live integration test over both
runner code paths (`tests/integration/test_lakehouse_orchestration.py`).

## Phase status

| Phase | Scope | Status |
|---|---|---|
| 0 | Bootstrap and engineering standards | done |
| 1 | Core infrastructure (PostgreSQL, MinIO, Polaris, Trino, Iceberg) | done |
| 2 | OLTP model and deterministic data generator | done |
| 3 | Batch ingestion: REST APIs + files/S3 | done |
| 4 | Airflow orchestration | done |
| 5 | dbt + Trino: Bronze -> Silver -> Gold | done |
| 6 | ClickHouse serving layer | done |
| 7 | Apache Superset | done |
| 8 | CDC: Debezium -> Kafka -> Iceberg | next |
| 9 | Clickstream + Spark | not started |
| 10 | Data quality, contracts, failure engineering | not started |
| 11 | Iceberg maintenance and performance | not started |
| 12 | Observability and lineage | not started |
| 13 | GitHub Actions CI/CD v2 | not started |
| 14 | Data Vault 2.0 mini-domain | not started |
| 15 | Production simulation / capstone | not started |
| 16 | Airflow 2 -> 3 migration exercise | not started |
| 17 | dbt Core 1.10 -> dbt v2 migration exercise | deferred until stable dbt v2 + confirmed Trino compatibility |
| 18 | GitLab CI migration | not started |

### Phase 5 slice detail

| Slice | Scope | Status |
|---|---|---|
| 1 | Bronze loader (Iceberg via Polaris) | done |
| 2 | Silver/Gold Kimball model, SCD2 `dim_customer`, reconciliation tests | done |
| 3 | Analytics marts + dataset-triggered orchestration | done |

### Phase 6 slice detail

| Slice | Scope | Status |
|---|---|---|
| 1 | ClickHouse service (`bi` profile), users, migrations, idempotent publisher for `mart_daily_sales` | done |
| 2 | Remaining 3 marts, engine/ORDER BY/partitioning design, reconciliation + full rebuild | done |
| 3 | Airflow dataset-triggered publication, Trino-vs-ClickHouse benchmark, runbook | done |

### Phase 7 slice detail

| Slice | Scope | Status |
|---|---|---|
| 1 | Superset platform: custom image, `bi`-profile services, connections, idempotent bootstrap (ADR 0005) | done |
| 2 | BI-as-code loop (sanitized bundles, fixed connection UUIDs) + Sales dashboard + canary test | done |
| 3 | Executive/Customer/Marketing dashboards, Trino ad-hoc path, runbook completion, README gallery scaffolding | done; screenshots are a manual follow-up (no browser in the agent environment) |

## Deferred / follow-ups

- dbt v2 migration is scheduled for Phase 17 after a stable release and a
  confirmed Trino path. Preparation includes migrating 16 generic-test
  definitions to the required `arguments` property and clearing dbt
  deprecation warnings on the v1 baseline.

Open follow-ups from the 2026-09-27 restart-verification postmortem
(milestone "Reliability debt (post-Phase 7)"):

- Bronze daily loads leave Iceberg snapshot history unbounded (order_items
  hit metadata v194 during the 2026-09-27 rebuild churn); expiration and
  maintenance stay Phase 11 scope — recorded there as an early trigger
  example.
- Superset dashboard screenshots: the README gallery table and capture
  procedure are in place (`docs/screenshots/README.md`); the PNG files are
  a manual step — no browser exists in the agent environment.
- Funnel mart in ClickHouse: deferred until Phase 9 clickstream data
  exists — Phase 6 publishes the four existing Gold marts only (plan
  decision, recorded at slice-2 completion).
- Branch reconciliation: `feature/phase5-followups` (forked before slice 2)
  carries a stale-watermark purge CLI, lockfile alignment, and a
  bronze-teardown cleanup; its polaris-init/compose/smoke fix (grant
  `CATALOG_MANAGE_CONTENT`, `DROP_WITH_PURGE_ENABLED`, smoke-core drop guard)
  is already in the slice-3 line — merge or rebase the remainder against it.
- MinIO images unpullable: `minio/minio` / `minio/mc` were removed from
  Docker Hub and dl.min.io returns 410; the local stack runs on locally
  built images from sha256-verified GitHub-release binaries, tagged under
  the pinned names. Follow-up issue (+ADR if the S3 store changes) to make
  `make up` reproducible again.
- Stale comment in `tests/integration/test_bronze_load.py` about Polaris
  denying DROP — obsolete after the purge/grant fix; cleanup exists as
  05e1fd3 on the followups branch.
- Bronze ingestion for the 4 file-based sources — supplier files land in
  MinIO (landing/archive) but are not loaded into Iceberg Bronze; the Bronze
  loader currently covers the PostgreSQL snapshot and API sources only.
  Follow-up issue.
- dbt model contracts as enforcement — depends on dbt-trino contract support;
  the fallback is YAML documentation plus tests.
