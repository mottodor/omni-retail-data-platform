# OmniRetail Data Platform

Production-like data engineering portfolio project: batch ingestion, Iceberg lakehouse (Trino + dbt), ClickHouse serving layer, BI with Superset, orchestration with Airflow, CDC via Debezium + Kafka, observability and lineage — on a local Docker Compose stack.

## Business problem

**OmniRetail** is a fictional e-commerce company. The platform answers questions such as:

- How do GMV, Revenue, Margin, and AOV evolve?
- What is the visit → product_view → cart → checkout → purchase funnel?
- Which products and categories generate the highest margin?
- How do marketing campaigns perform (CTR, CAC, ROAS)?
- What is customer retention/LTV?
- Which deliveries are late?
- Are there discrepancies between orders and payments?

## Architecture

```text
SOURCES
  PostgreSQL OLTP ---- Debezium ---- Kafka -------------------+
  REST APIs ---------------- Airflow/Python ------------------+
  S3/CSV/JSON/Parquet ------- Airflow ------------------------+--> MinIO/S3
  Web/App events ------------ Kafka --------------------------+      |
                                                                   Iceberg
                                                          Bronze -> Silver -> Gold
                                                                     |
                                                           Trino + dbt Core
                                                                     |
                                                +--------------------+------------------+
                                                |                                       |
                                           Iceberg Gold                           ClickHouse
                                            source of truth                      serving layer
                                                                                       |
                                                                                   Superset

ORCHESTRATION: Airflow 2.11.2
CATALOG: Apache Polaris (Iceberg REST catalog)
BIG DATA: Spark for heavy files/clickstream/sessionization
QUALITY: dbt tests + custom SQL/Python reconciliation
LINEAGE: OpenLineage + Marquez
MONITORING: Prometheus + Grafana
DEVOPS: GitHub + GitHub Actions + Docker Compose
LATER: GitLab CI, Kubernetes, Airflow 3 migration
```

Iceberg is the analytical source of truth; ClickHouse is a derived serving layer that can always be rebuilt from Iceberg Gold.

## Status

- [x] Phase 0 — repository bootstrap and engineering standards (this phase)
- [ ] Phase 1 — core infrastructure (PostgreSQL, MinIO, Polaris, Trino, Iceberg)
- [ ] Phase 2+ — see `ROADMAP.md`

## Prerequisites

- Linux/WSL2 (target machine: Windows 11 + WSL2, 32 GB RAM)
- [uv](https://docs.astral.sh/uv/) (Python package manager)
- Python 3.12 (installed automatically by uv)
- Docker + Docker Compose (required from Phase 1)
- make

## Setup

```bash
# Copy and fill the environment file (placeholders only; never commit .env)
cp .env.example .env

# Create the virtualenv, install dependencies, install pre-commit hooks
make setup
```

## Commands

```bash
make setup   # uv sync + pre-commit install
make lint    # ruff check + ruff format --check + mypy
make test    # pytest
make unit    # alias for make test
make up      # start services (from Phase 1, when docker-compose.yml exists)
make down    # stop services
make logs    # follow service logs
make reset   # DESTRUCTIVE: down -v, destroys local volumes
```

## CI

GitHub Actions runs on every push and pull request: ruff check, ruff format, mypy (non-blocking at bootstrap), pytest. The `docker compose config` check activates automatically once `docker-compose.yml` exists (Phase 1).
