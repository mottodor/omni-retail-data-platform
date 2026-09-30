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

Phase 8 (CDC: Debezium -> Kafka -> Iceberg) is next. The first slice must
establish PostgreSQL logical replication, Debezium/Kafka persistence, and a
restart-safe Bronze path for customers, orders, and payments. Execution plan:
[`docs/plans/active.md`](docs/plans/active.md).

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

## Tracked technical debt

| ID | Debt / risk | Status | Disposition |
|---|---|---|---|
| TD-001 | `make up` is not reproducible on a clean host because the pinned MinIO images disappeared from Docker Hub; current workstations use locally built images from checksum-verified release binaries. | open | [#17](https://github.com/mottodor/omni-retail-data-platform/issues/17): restore a reproducible image supply; changing the S3 store requires an ADR. |
| TD-002 | Four supplier file sources stop in MinIO landing/archive and are not loaded into Iceberg Bronze. | open | [#18](https://github.com/mottodor/omni-retail-data-platform/issues/18): add manifest-driven, idempotent file-source Bronze loading. |
| TD-003 | Bronze daily loads leave Iceberg snapshot history unbounded; rebuild churn previously drove `order_items` metadata to v194. | scheduled: Phase 11 | Add snapshot expiration/maintenance with retention and rollback safety. |
| TD-004 | dbt model contracts are documented and tested but not enforced by dbt because adapter support is incomplete. | blocked upstream | Track dbt-trino contract support; keep YAML documentation plus tests as the fallback. |
| TD-005 | The dbt v1 project has 16 generic-test definitions using syntax that must move under `arguments` for dbt v2. | deferred: Phase 17 | Migrate after stable dbt v2 and confirmed Trino compatibility; clear v1 deprecation warnings first. |

## Deferred / follow-ups

- Superset dashboard screenshots are still a manual step; the README gallery
  and capture procedure are ready in `docs/screenshots/README.md`.
- Funnel, conversion, ROAS, and CAC marts remain deferred to Phase 9 because
  clickstream and attribution data do not exist yet.
