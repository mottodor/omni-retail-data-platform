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

Phase 6 (ClickHouse serving layer) — sliced into three vertical slices per
`docs/plans/active.md` (read it first and resume from its checklist).
ADR 0004 (deployment + publication mechanism) is accepted; **slice 1 is
done** (compose `bi` profile + `clickhouse-init` migrations ledger,
`omni_publisher`/`superset_reader` accounts, idempotent staging-swap
publisher + CLI for `mart_daily_sales`, unit + live integration tests).
Next: slice 2 — remaining 3 marts, per-mart engine/ORDER BY/partitioning
design, Gold-vs-CH reconciliation, one-command full rebuild. Funnel mart is
deferred until Phase 9 clickstream data exists.

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
| 6 | ClickHouse serving layer | in progress (slice 2 of 3) |
| 7 | Apache Superset | not started |
| 8 | CDC: Debezium -> Kafka -> Iceberg | not started |
| 9 | Clickstream + Spark | not started |
| 10 | Data quality, contracts, failure engineering | not started |
| 11 | Iceberg maintenance and performance | not started |
| 12 | Observability and lineage | not started |
| 13 | GitHub Actions CI/CD v2 | not started |
| 14 | Data Vault 2.0 mini-domain | not started |
| 15 | Production simulation / capstone | not started |
| 16 | Airflow 2 -> 3 migration exercise | not started |
| 17 | GitLab CI migration | not started |

### Phase 5 slice detail

| Slice | Scope | Status |
|---|---|---|
| 1 | Bronze loader (Iceberg via Polaris) | done |
| 2 | Silver/Gold Kimball model, SCD2 `dim_customer`, reconciliation tests | done |
| 3 | Analytics marts + dataset-triggered orchestration | done |

### Phase 6 slice detail (plan: `docs/plans/active.md`)

| Slice | Scope | Status |
|---|---|---|
| 1 | ClickHouse service (`bi` profile), users, migrations, idempotent publisher for `mart_daily_sales` | done |
| 2 | Remaining 3 marts, engine/ORDER BY/partitioning design, reconciliation + full rebuild | next |
| 3 | Airflow dataset-triggered publication, Trino-vs-ClickHouse benchmark, runbook | not started |

## Deferred / follow-ups

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
