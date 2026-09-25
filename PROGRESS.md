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

Last updated: 2026-09-25.

## Current focus

Phase 5 slice 3 tail — lakehouse orchestration. Already merged:
`int_orders_fx` and the four analytics marts with EUR normalization.
Also merged: watermark-driven Bronze `run-new` (loads archived logical dates
past `max(_batch_date)` per source; Bronze itself is the watermark state).
Remaining:

- `transform_lakehouse` DAG triggered by the `lakehouse://bronze` dataset
  (`load_bronze` -> `dbt build`);
- live integration test covering the transform DAG;
- docs: README slice-3 section and `docs/data-model.md` marts tables.

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
| 3 | Analytics marts + dataset-triggered orchestration | in progress — see Current focus |

## Deferred / follow-ups

- Bronze ingestion for the 4 file-based sources — supplier files land in
  MinIO (landing/archive) but are not loaded into Iceberg Bronze; the Bronze
  loader currently covers the PostgreSQL snapshot and API sources only.
  Follow-up issue.
- dbt model contracts as enforcement — depends on dbt-trino contract support;
  the fallback is YAML documentation plus tests.
