# ADR 0001 — Project architecture and technology stack

## Title

Project architecture and technology stack for OmniRetail Data Platform.

## Status

Accepted.

## Context

The project is a production-like e-commerce data platform designed to demonstrate practical Data Engineering skills on a local workstation (Windows 11 + WSL2, 32 GB RAM, Docker Compose as primary runtime).

The roadmap is a vertical-slice-first build:

```text
PostgreSQL -> batch extraction -> MinIO -> Iceberg Bronze -> Trino + dbt -> Gold mart -> ClickHouse -> Superset
```

Only after the first slice works end-to-end do Kafka/Debezium CDC, Spark, observability, and lineage enter the picture.

## Decision

- **Python 3.12** baseline; **uv** as the package and dependency manager with a committed `uv.lock`.
- **Apache Airflow 2.11.2** for orchestration; Airflow coordinates work, dbt owns transformation SQL.
- **Apache Iceberg** as the analytical source of truth; **Apache Polaris** as the Iceberg REST catalog.
- **MinIO** as local S3-compatible object storage.
- **Trino 476+** (initial pinned target: Trino 483) as the primary SQL engine; **dbt Core** for relational analytical transformations (staging/intermediate/core/marts).
- **ClickHouse** as a low-latency derived serving layer, rebuildable from Iceberg Gold.
- **Apache Superset** as business BI (Superset → ClickHouse primary; Superset → Trino → Iceberg for ad-hoc).
- **Apache Kafka + Debezium** for PostgreSQL CDC and event transport (introduced in later phases).
- **Apache Spark** only for workloads that justify distributed processing.
- **Prometheus + Grafana** for platform observability; **OpenLineage + Marquez** for lineage.
- **Docker Compose** with logical profiles (`core`, `orchestration`, `streaming`, `spark`, `observability`, `bi`) to respect the limited local RAM.
- **GitHub + GitHub Actions** for CI/CD; images pushed to GHCR.

## Alternatives considered

- **Warehouse-only approach** (PostgreSQL → dbt directly, no lakehouse): rejected because the project's goal is to demonstrate lakehouse engineering (Iceberg maintenance, snapshots, partition evolution).
- **Hive Metastore catalog** instead of Polaris: rejected in favor of the modern Iceberg REST catalog pattern.
- **Spark as default transformation engine**: rejected; Trino + dbt covers relational transformations with a smaller footprint.
- **ClickHouse as the only persistent store**: rejected; it is a serving layer and must be rebuildable from Iceberg Gold.
- **Kubernetes from day one**: rejected; Docker Compose is sufficient for the local workstation and Kubernetes is a later exercise.

## Consequences

- The stack is heavier than a pure-dbt project, but each technology serves a concrete learning goal.
- Strict phase gating (see `AGENTS.md` §51) is required to avoid running the full stack simultaneously on a 24 GB WSL2 budget.
- Iceberg/Polaris/Trino integration must be verified early (Phase 1 smoke test) because it is the architectural keystone.

## Rollback / migration considerations

- Compose profiles allow disabling a subsystem without rework.
- ClickHouse can always be rebuilt from Iceberg Gold if its state is lost or corrupted.
- The roadmap reserves migration exercises (Airflow 2 → 3, dbt Core 1.10 → dbt v2, GitHub Actions → GitLab CI) as later phases; those will be preceded by their own ADRs.
