# PROGRESS.md — current project state

Single home of **current state**: what is done, what is in flight, and what is
deliberately deferred. Plans and acceptance criteria live in `ROADMAP.md`;
architectural decisions live in `docs/adr/`; the audit trail of *when*
things happened is git history — this file only indexes *now*.

Contract:

- status only — no narratives, no plan copies; keep under ~100 lines;
- updated in the same PR/commit as the work it reflects (AGENTS.md §6, DoD);
- read at session start (AGENTS.md §5.1); updated whenever a task changes
  phase/slice status, the current focus, or the deferred list (AGENTS.md §49).

Last updated: 2026-09-26.

## Current focus

Active plan: `docs/plans/active.md` — task 4 of slice 3, tail:
`docs/data-model.md` marts tables. The README half is done: restructured
2026-09-26 by user request into a capability-oriented, phase-free document
(state as of today; target architecture marked "target"; grouped Commands;
new Project layout section; slice-3 lakehouse orchestration and marts
content included). Task 3 (live integration test over the `run_bronze_load`
-> `run_dbt_build` runners, incl. idempotency leg) is done.

Phase 5 slice 3 tail — lakehouse orchestration. All pipeline work merged:
`int_orders_fx` + four analytics marts with EUR normalization;
watermark-driven Bronze `run-new`; dataset-triggered pair `load_bronze` /
`transform_lakehouse` (+ DAG structure tests); live integration test
`test_lakehouse_orchestration.py` with shared seeding helpers in
`tests/integration/lakehouse_seed.py`.

Environment fix that unblocked validation on a fresh stack (volumes wiped by
a Docker Desktop reset): ported the tested `feature/phase5-followups` infra
commits (grant `CATALOG_MANAGE_CONTENT` to `catalog_admin` in polaris-init,
`DROP_WITH_PURGE_ENABLED` compose flag, smoke-core drop guard) into this
branch's working tree.

## Phase status

| Phase | Scope | Status |
|---|---|---|
| 0 | Bootstrap and engineering standards | done |
| 1 | Core infrastructure (PostgreSQL, MinIO, Polaris, Trino, Iceberg) | done |
| 2 | OLTP model and deterministic data generator | done |
| 3 | Batch ingestion: REST APIs + files/S3 | done |
| 4 | Airflow orchestration | done |
| 5 | dbt + Trino: Bronze -> Silver -> Gold | **in progress** |
| 6 | ClickHouse serving layer | not started |
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
| 3 | Analytics marts + dataset-triggered orchestration | in progress — docs task 4 remains |

## Deferred / follow-ups

- Branch reconciliation: `feature/phase5-followups` (forked before slice 2)
  carries the purge/grant infra fix, a stale-watermark purge CLI, lockfile
  alignment, and a bronze-teardown cleanup; its polaris-init/compose/smoke
  changes are already in this branch's working tree — merge or rebase it
  against the slice-3 line.
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
