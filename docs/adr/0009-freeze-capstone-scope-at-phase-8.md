# ADR 0009 — Freeze the OmniRetail capstone scope at Phase 8

## Status

Accepted.

## Context

OmniRetail was initially planned as an expanding, production-like educational
data platform. ADR 0001 and the original roadmap reserved later phases for
clickstream processing with Spark, broader data quality and failure
engineering, Iceberg maintenance, platform observability and lineage, CI/CD
hardening, Data Vault, production simulation, and migration exercises.

Phases 0–8 now form a coherent end-to-end capstone. The delivered system covers
batch ingestion and PostgreSQL CDC through Debezium and Kafka, immutable
Iceberg Bronze storage, typed and delete-aware Silver state, dbt Gold models,
Airflow orchestration at a stable Kafka-offset boundary, atomic publication to
ClickHouse, and Superset BI. It can therefore demonstrate a complete data path
from source mutation to an analytical result without the unimplemented later
phases.

Continuing to add unrelated platform concerns to the same repository would make
the architecture broader while reducing the focus available for each stage of
data processing. It would also make the portfolio narrative and the boundaries
of individual engineering decisions less clear. New learning topics are better
served by smaller, independently scoped projects.

The repository still has documented technical debt and intentionally omitted
production concerns. Completing the selected feature scope does not imply that
the platform is debt-free or suitable for production operation.

## Decision

OmniRetail feature development ends at the completed Phase 8 scope.

The repository is now a maintenance-only portfolio project. The canonical
classification is:

> The project is a production-like educational capstone, not a
> production-ready platform.

The following rules apply:

1. Phases 0–8 define the final feature scope and the implemented architecture.
2. The detailed Phase 9–18 specifications are removed from the active roadmap.
   They are not deferred OmniRetail backlog.
3. Topic-focused continuation work is separated into independent repositories
   in the same profile. OmniRetail documentation does not maintain names or
   direct links for those repositories.
4. Existing implemented components remain in place: PostgreSQL, MinIO,
   Polaris, Trino, Iceberg, Airflow, dbt, Kafka, Debezium, ClickHouse, Superset,
   Docker Compose, and GitHub Actions.
5. Normal changes to this repository are limited to documentation, bug fixes,
   security and dependency maintenance, and resolution of technical debt in
   the delivered scope.
6. A technical-debt item associated with a removed later phase may still be
   addressed when it narrowly protects or repairs delivered behavior and does
   not introduce a new platform subsystem or restore that phase as a broader
   initiative.
7. New feature domains, platform services, migration exercises, or former
   Phase 9–18 initiatives are out of scope.
8. Reopening or expanding the feature scope requires a new ADR that explicitly
   supersedes this decision. A GitHub issue alone is insufficient.
9. Remote issues that exist only to implement Phases 9–18 should be closed as
   `not planned`, with ADR 0009 recorded as the reason; corresponding milestones
   should be closed or retired. Ideas may be recreated in the independently
   scoped repositories when appropriate.

This decision partially supersedes ADR 0001 only where ADR 0001 commits the
repository to unimplemented future expansion. ADR 0001 remains authoritative
for the architecture and technology responsibilities already delivered through
Phase 8.

## Alternatives considered

### Continue the original roadmap in this repository

Rejected. It would preserve the initial plan but continue increasing the number
of platform concerns in one educational project, weakening focus and making the
capstone harder to explain and operate.

### Implement only selected parts of Phases 9–18

Rejected. A selective continuation would still leave an open-ended backlog and
blur whether omitted phases were intentionally excluded or merely unfinished.

### Keep Phases 9–18 as an indefinite backlog

Rejected. This would make a completed Phase 8 capstone appear incomplete and
would continue directing contributors and coding agents toward work that is no
longer intended for this repository.

### Move later learning topics into independent projects

Accepted. Smaller repositories can give clickstream and Spark, reliability,
Iceberg operations, observability and lineage, CI/CD, modeling alternatives,
or migration exercises their own explicit goals and acceptance criteria.

### Remove implemented components to simplify OmniRetail further

Rejected. The Phase 8 stack already forms the intended end-to-end demonstration.
Removing CDC, orchestration, serving, or BI components would discard completed
work and weaken the capstone rather than clarify its boundary.

## Consequences

Positive consequences:

- the repository has a clear and defensible completion boundary;
- the portfolio story matches the architecture that actually exists;
- documentation, tests, and maintenance can focus on the delivered data path;
- future learning projects can explore one concern in greater depth without
  inheriting the whole OmniRetail stack;
- local operational and cognitive load no longer grows through additional
  services.

Negative and neutral consequences:

- OmniRetail will not demonstrate Spark clickstream processing, platform-wide
  observability, OpenLineage/Marquez lineage, Data Vault, or the planned
  migration exercises;
- the original production-simulation and advanced failure-engineering goals are
  not completion criteria for this repository;
- known limitations and technical debt remain and must be disclosed rather
  than hidden by the capstone label;
- some debt may remain unresolved indefinitely when fixing it would require a
  new subsystem or effectively restart a removed phase;
- documentation and remote project-management artifacts must be updated so
  that they do not continue presenting Phases 9–18 as active work.

## Rollback / migration considerations

No runtime or data migration is required because this decision removes only
unimplemented future scope. The delivered services, schemas, contracts, and
data paths remain unchanged.

The documentation migration consists of:

- replacing target-architecture claims with the implemented Phase 8
  architecture;
- removing detailed Phase 9–18 roadmap content;
- marking the repository as maintenance-only;
- retaining open debt as explicit limitations;
- closing remote Phase 9–18 issues as `not planned` and retiring their
  milestones;
- preserving the original roadmap history in Git rather than in an active
  appendix.

If the repository is deliberately expanded later, the change must begin with a
new ADR that supersedes ADR 0009, defines the concrete problem being solved,
restores only the necessary roadmap scope, and explains why an independent
repository is no longer the better boundary.
