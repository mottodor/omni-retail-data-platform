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

Phase 8 is complete and the repository is maintenance-only under ADR 0009.
Portfolio documentation, dashboard evidence, roadmap scope, and normative agent
rules are aligned with the final Phase 0–8 boundary. There is no active feature
phase; maintenance priorities are the tracked technical-debt items below.

## Phase status

| Phase | Scope | Status |
| --- | --- | --- |
| 0 | Bootstrap and engineering standards | done |
| 1 | Core infrastructure (PostgreSQL, MinIO, Polaris, Trino, Iceberg) | done |
| 2 | OLTP model and deterministic data generator | done |
| 3 | Batch ingestion: REST APIs + files/S3 | done |
| 4 | Airflow orchestration | done |
| 5 | dbt + Trino: Bronze -> Silver -> Gold | done |
| 6 | ClickHouse serving layer | done |
| 7 | Apache Superset | done |
| 8 | CDC: Debezium -> Kafka -> Iceberg | done — final feature phase |

## Tracked technical debt

| ID | Debt / risk | Status | Disposition |
| --- | --- | --- | --- |
| TD-001 | `make up` is not reproducible on a clean host because the pinned MinIO images disappeared from Docker Hub; current workstations use locally built images from checksum-verified release binaries. | open | [#17](https://github.com/mottodor/omni-retail-data-platform/issues/17): restore a reproducible image supply; changing the S3 store requires an ADR. |
| TD-002 | Four supplier file sources stop in MinIO landing/archive and are not loaded into Iceberg Bronze. | open — eligible maintenance | [#18](https://github.com/mottodor/omni-retail-data-platform/issues/18): may add manifest-driven, idempotent Bronze loading inside the delivered batch-ingestion scope; no phase commitment. |
| TD-003 | Bronze daily loads leave Iceberg snapshot history unbounded; rebuild churn previously drove `order_items` metadata to v194. | open — eligible maintenance | Safe retention/expiration may be added as bounded maintenance with rollback protection; no scheduled phase. |
| TD-004 | dbt model contracts are documented and tested but not enforced by dbt because adapter support is incomplete. | blocked upstream | Track dbt-trino contract support; keep YAML documentation plus tests as the fallback. |
| TD-005 | The dbt v1 project has 13 generic-test definitions using syntax that must move under `arguments` for dbt v2. | accepted capstone limitation | Keep the pinned dbt 1.10 baseline. Syntax cleanup may be maintenance; a dbt major-version migration requires an ADR superseding ADR 0009. |
| TD-006 | Insert-only CDC microbatches accumulate Iceberg data files and snapshots; sustained streams need compaction and retention. | open — eligible maintenance | Compaction/retention may be added only as bounded maintenance that preserves raw event semantics; no scheduled phase. |

## Scope exclusions

- Clickstream, funnel attribution, CAC, and ROAS are intentional non-goals, not
  deferred OmniRetail work.
- Distributed processing, dedicated monitoring/lineage services, alternate
  modeling domains, and platform migration exercises belong in independently
  scoped repositories unless a new ADR explicitly supersedes ADR 0009.
