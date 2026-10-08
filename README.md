# OmniRetail Data Platform

A production-like educational data engineering capstone for a fictional
e-commerce company. OmniRetail combines batch ingestion and PostgreSQL CDC with
an Iceberg lakehouse, dbt dimensional modeling, Airflow orchestration,
ClickHouse serving, and Superset BI on a local Docker Compose stack.

**Status: Phase 8 capstone complete; maintenance-only.** Feature development
ends at the delivered Phase 0–8 scope. See
[ADR 0009](docs/adr/0009-freeze-capstone-scope-at-phase-8.md) and the
[completed roadmap](ROADMAP.md).

> This project is a production-like educational capstone, not a
> production-ready platform.

## What this project demonstrates

- **Heterogeneous batch ingestion** from PostgreSQL snapshots, paginated REST
  APIs, CSV, JSON, Parquet, and XLSX sources.
- **Restart-safe CDC** from PostgreSQL WAL through Debezium and Kafka into an
  insert-only Iceberg Bronze event table.
- **Lakehouse modeling** with MinIO, Apache Iceberg, Apache Polaris, Trino, and
  dbt staging/intermediate/core/mart layers.
- **CDC-aware analytics** with typed events, delete-aware current state,
  customer SCD Type 2, hard-delete handling, and recreate semantics.
- **Safe orchestration** with Airflow dataset triggers and analytical refreshes
  pinned to a healthy, stable Kafka-offset boundary.
- **Rebuildable serving** through atomic full-snapshot publication from Iceberg
  Gold to ClickHouse.
- **BI as code** with sanitized Superset datasets and dashboards committed to
  the repository.
- **Engineering controls** including deterministic fixtures, idempotent paths,
  reconciliation tests, recovery runbooks, pinned dependencies, and CI.

## Business outputs

The delivered analytical layer supports:

- GMV, revenue, margin, orders, and AOV over time;
- sales and margin by category and region;
- customer LTV, new/repeat composition, and customer segments;
- delivery transit time and status mix by carrier;
- marketing spend, impressions, clicks, CTR, CPC, CPM, and budget utilization;
- order-to-payment reconciliation;
- analytical propagation of customer, order, and payment changes and deletes.

Clickstream funnel, conversion attribution, CAC, and ROAS are not claimed:
the required clickstream and attribution sources are intentionally outside this
repository's final scope.

## Implemented architecture

```text
SOURCES

PostgreSQL OLTP
  ├── batch snapshots ── MinIO archive ── Bronze loader ──┐
  └── WAL ── Debezium ── Kafka ── CDC consumer ──────────┤
                                                          ├──> Iceberg Bronze
Mock REST APIs ── Airflow/Python ── MinIO archive ── Bronze loader ──────┤
Supplier files ── Airflow/Python ── MinIO archive ── Bronze loader ──────┘

Iceberg Bronze
      │
      │  Trino + dbt Core
      ▼
Typed/delete-aware Silver
      │
      ▼
Kimball Gold facts/dimensions
      │
      ▼
Analytics marts in Iceberg
      │
      │  atomic full-snapshot publication
      ▼
ClickHouse serving layer
      │
      ▼
Apache Superset dashboards

ORCHESTRATION: Airflow 2.11.2
CATALOG: Apache Polaris
OBJECT STORAGE: MinIO
RUNTIME: Docker Compose profiles
CI: GitHub Actions
```

### Component responsibilities

| Component | Responsibility |
| --- | --- |
| PostgreSQL | OLTP source with deterministic initial data and mutation workload |
| Python ingestion | API, file, snapshot, and Bronze-loading flows |
| Debezium | PostgreSQL WAL change capture for customers, orders, and payments |
| Kafka | Persistent CDC event transport |
| CDC consumer | Restart-safe insertion of raw CDC events into Iceberg Bronze |
| MinIO | S3-compatible landing, archive, rejected, and lakehouse storage |
| Iceberg | Analytical source of truth for Bronze, Silver, Gold, and marts |
| Polaris | Iceberg REST catalog |
| Trino | SQL compute over Iceberg and Superset ad-hoc query path |
| dbt Core | Typing, deduplication, SCD2, facts, dimensions, marts, and tests |
| Airflow | Batch orchestration and stable-boundary analytical refresh |
| ClickHouse | Derived low-latency serving copy, rebuildable from Iceberg Gold |
| Superset | BI dashboards through ClickHouse and ad-hoc SQL through Trino |

Iceberg is the analytical source of truth. ClickHouse contains no unique
business state and can be rebuilt from Gold.

## End-to-end capstone flow

1. The deterministic generator creates an OLTP baseline in PostgreSQL.
2. Batch extractors preserve raw API, PostgreSQL snapshot, and supplier-file
   payloads in the MinIO archive and load manifest-backed sources into Iceberg
   Bronze.
3. Debezium captures customer, order, and payment mutations from PostgreSQL WAL
   and writes them to table-specific Kafka topics.
4. The CDC consumer writes raw events to Iceberg and commits Kafka offsets only
   after a successful Iceberg operation.
5. Airflow waits for healthy Debezium tasks, an active consumer, lag zero, and
   an unchanged exclusive high-watermark boundary.
6. dbt builds typed/delete-aware Silver state, Kimball Gold models, and four
   analytics marts at that exact boundary.
7. Critical dbt tests and business reconciliation must pass before publication.
8. The publisher loads complete mart snapshots into ClickHouse staging twins
   and atomically swaps them into service.
9. Superset reads the refreshed KPI state through a read-only ClickHouse user.
10. Re-running the same logical work converges without duplicate business data.

## Correctness and reliability design

### Batch paths

- Raw API pages and accepted files are preserved unchanged in MinIO.
- Logical dates, not wall-clock time, define object paths and Bronze
  partitions; file batch identity is content-addressed by checksum.
- File checksums and canonical completed manifests provide duplicate detection,
  load authorization, and audit metadata.
- PostgreSQL snapshots use keyset pagination and advance durable watermarks only
  after a successful upload.
- Bronze loads verify manifest ownership, checksums, accepted/rejected counts,
  and deterministic source-object coordinates before DML. File loads scan
  completed manifests so late files for older logical dates remain eligible.
- Invalid files and rows are quarantined with machine-readable reasons.

### CDC path

- Debezium initial `r` records provide the pre-streaming baseline.
- Raw Bronze preserves source LSN, transaction metadata, Kafka coordinates, and
  delete operations.
- Current state is ordered by source LSN and Kafka offset, not event or ingestion
  timestamps.
- Duplicate delivery does not change final analytical state.
- Winning deletes remove current Gold keys; a later create starts a new
  lifecycle.
- Snapshot-backed order items and shipments are filtered through the live CDC
  order set to suppress stale cascade-deleted children.

### Analytical refresh and serving

- Airflow freezes a stable Kafka frontier before dbt starts.
- `max_active_runs=1` serializes the boundary/build/publication chain.
- New CDC events above the frozen boundary wait for a later refresh.
- ClickHouse publication uses staging tables plus atomic `EXCHANGE TABLES`;
  readers never observe a partial mart refresh.
- Routine refresh, retry, and full rebuild use the same publisher code path.

Detailed semantics are documented in the
[data model](docs/data-model.md), [data contracts](docs/data-contracts.md), and
[CDC runbook](docs/runbooks/kafka-cdc.md).

## Dashboard gallery

Superset assets are committed as sanitized bundles under `superset/assets/`.
The delivered dashboards are:

- **Sales** — GMV/revenue/margin trends, AOV, category and region breakdowns;
- **Executive** — business KPI and delivery overview;
- **Customer** — acquisition cohorts, new/repeat mix, repeat rate, LTV, and top
  customers;
- **Marketing** — spend, budget utilization, CTR, CPC/CPM, and campaign
  scorecards.

### Sales

![Superset Sales dashboard](docs/screenshots/sales-dashboard.jpg)

### Executive

![Superset Executive dashboard](docs/screenshots/executive-dashboard.jpg)

### Customer

![Superset Customer dashboard](docs/screenshots/customer-dashboard.jpg)

### Marketing

![Superset Marketing dashboard](docs/screenshots/marketing-dashboard.jpg)

The reproducible capture procedure is documented in
[`docs/screenshots/README.md`](docs/screenshots/README.md).

## Local setup

### Prerequisites

- Linux or WSL2; the target workstation is Windows 11 + WSL2 with 32 GB RAM;
- Docker with Docker Compose;
- [`uv`](https://docs.astral.sh/uv/);
- `make`.

Recommended WSL2 allocation:

```ini
[wsl2]
memory=24GB
processors=6
swap=8GB
```

### Python environment and core smoke test

#### One-time workstation setup

```bash
cp .env.example .env      # fill local values; never commit .env
make setup                # uv sync + pre-commit install
```

#### First-time data bootstrap

Use this sequence only for a new set of local volumes:

```bash
make up                   # core profile, health-gated
make generate-oltp        # strict deterministic initial load
make smoke-core           # Trino -> Polaris -> Iceberg -> MinIO
```

`generate-oltp` intentionally fails when any OLTP table is already populated;
that is a data-loss guard, not a failed routine restart. Its destructive
`--truncate-oltp-data` option is not a startup tool: truncating PostgreSQL must
be coordinated with the explicit CDC transport/Bronze recovery procedure in the
[Kafka/CDC runbook](docs/runbooks/kafka-cdc.md).

`make down` stops core services while preserving named volumes. `make reset` is
explicitly destructive and couples the PostgreSQL/core reset with Kafka/Connect
transport deletion so stale source offsets cannot survive a new source volume.
It preserves BI and Airflow metadata volumes but deletes MinIO/Iceberg data.

> Known limitation: clean-host bootstrap currently depends on restoring a
> reproducible supply for pinned MinIO images. See TD-001 in
> [PROGRESS.md](PROGRESS.md); this README does not claim unconditional
> clean-clone reproducibility while that debt remains open.

### Compose profiles

The stack is intentionally profile-based so a 32 GB workstation does not need
to run every service continuously.

| Profile | Start command | Services |
| --- | --- | --- |
| Core | `make up` | PostgreSQL, MinIO, Polaris, Trino, mock API |
| Streaming | `make streaming-up` | Kafka, Debezium, CDC consumer |
| BI | `make bi-up` | ClickHouse, Superset, metadata/init services |
| Orchestration | `make airflow-up` | Airflow scheduler/webserver and metadata DB |

#### Routine full-environment startup

With populated volumes, restart the complete environment without rerunning the
strict initial loader:

```bash
make up
make streaming-up
make bi-up
make airflow-up
make streaming-status
```

An already populated OLTP source is the expected successful state for this
routine path. `make streaming-status` exits non-zero unless the connector, every
task, and the active CDC consumer are healthy; non-zero lag is displayed as
catch-up progress and is not by itself a service-health failure.

Before the first analytical refresh on a new CDC deployment, complete the
initial-snapshot gate in the
[Kafka/CDC runbook](docs/runbooks/kafka-cdc.md). Do not run manual dbt or
publication commands concurrently with `transform_lakehouse`.

### Representative commands

```bash
# Source data (generate-oltp is first-time-only and rejects populated tables)
make generate-oltp
make mutate-oltp EVENTS=300
make seed-supplier-files ROWS=500 SEED=11

# Raw ingestion
make ingest-files ARGS="--source supplier-prices --date 2026-09-11"
make ingest-api ARGS="run --source fx-rates --date 2026-09-10"
make ingest-api ARGS="backfill --source fx-rates --from 2026-09-01 --to 2026-09-10"

# Lakehouse
make bronze-load ARGS="run-new"
make dbt-parse
make dbt-build
make dbt-test

# CDC
make streaming-status
make streaming-down       # preserves Kafka/Connect/slot/Bronze state
make streaming-reset      # destructive transport reset; clearly prompted

# Serving and BI
make serving-publish ARGS="--mart mart_daily_sales"
make serving-rebuild
make serving-benchmark

# Airflow
make airflow-test
make airflow-dag-test ARGS="transform_lakehouse 2026-09-18"
```

Use `make help` for the complete command list. Operational recovery procedures
live under [`docs/runbooks/`](docs/runbooks/).

## Analytical model

### Core dimensions and facts

- `dim_customer` — CDC-backed SCD Type 2 customer history;
- `dim_product`, `dim_date`, `dim_campaign`;
- `fact_orders`, `fact_payments` — CDC-backed current facts;
- `fact_order_items`, `fact_shipments` — snapshot-backed children gated by live
  CDC orders.

### Marts

- `mart_daily_sales` — date × category × region sales metrics;
- `mart_customer_ltv` — customer order, GMV, AOV, and segment metrics;
- `mart_marketing_roi` — campaign spend and engagement metrics; the historical
  table name is retained, but ROAS is not claimed without attribution data;
- `mart_delivery_performance` — carrier transit and status metrics.

Financial measures are normalized to EUR using order-date FX rates. Full grain,
key, and lifecycle semantics are in [`docs/data-model.md`](docs/data-model.md).

## Testing and CI

Local validation entry points:

```bash
make lint             # ruff check, format check, mypy
make test             # hermetic unit/contract tests
make dbt-parse        # offline dbt manifest validation
make airflow-test     # DAG pytest + import-error check in the Airflow image

docker compose config
make smoke-core       # requires live core
make integration      # core required; absent optional profiles are skipped
```

`make integration` is opt-in and never starts or resets services. A healthy
core profile is mandatory. Streaming and BI checks run only when their host
endpoints are reachable; otherwise they report actionable skips. To execute
rather than skip every BI data check, prepare the normal Gold and serving state:

```bash
make up
make bi-up
make dbt-build
make serving-rebuild
make integration
```

Once an optional service is reachable, unhealthy responses, bad credentials,
incomplete bootstrap, query failures, permission regressions, and reconciliation
differences fail the suite rather than being converted into skips.

The integration suite uses disposable lakehouse schemas and exact-key rollback
journals rather than resetting shared schemas. Catalog-aware teardown retries
only recognized transient Trino/Polaris visibility failures with a finite
budget. Covered boundaries include:

- all four supplier-file formats and APIs through real MinIO;
- PostgreSQL snapshot lifecycle and watermarks;
- manifest-backed Bronze loading, partial-commit recovery, and idempotent reruns;
- dbt core build and business reconciliation;
- Debezium/Kafka CDC recovery and duplicate handling;
- Gold-to-ClickHouse publication and rebuildability;
- Superset bootstrap, ClickHouse canary, and Trino SQL Lab connectivity;
- dataset-triggered Bronze → dbt orchestration.

GitHub Actions runs ruff, pytest, non-blocking mypy, offline dbt parse, Docker
Compose validation, and Airflow DAG tests. It does not claim a hosted full-stack
integration environment.

## Performance evidence

The repository includes a reproducible representative comparison of the same
aggregation over Trino/Iceberg and ClickHouse:

- [Phase 6 Trino vs ClickHouse benchmark](docs/benchmarks/phase6-trino-vs-clickhouse.md)

The recorded numbers are local-workstation observations for a small fixture,
not general engine performance claims. The report preserves query equivalence,
plans, and measurement context.

## Repository layout

```text
src/omni_retail/    Python package: generators, ingestion, Bronze, CDC, serving
postgres/           Idempotent OLTP schema
trino/              Trino and Iceberg catalog configuration
dbt/                staging -> intermediate -> core -> marts
airflow/            DAGs, datasets, runners, policy, DAG tests
clickhouse/          Versioned serving migrations
superset/            Sanitized BI assets as code
infrastructure/      Custom images, bootstrap, smoke, and recovery scripts
tests/               Unit, contract, and opt-in integration tests
docs/                ADRs, model, contracts, runbooks, benchmarks, screenshots
```

## Documentation map

| Document | Purpose |
| --- | --- |
| [ROADMAP.md](ROADMAP.md) | Final Phase 0–8 scope, completed phases, and acceptance criteria |
| [PROGRESS.md](PROGRESS.md) | Current maintenance status and open technical debt |
| [Architecture ADRs](docs/adr/README.md) | Technology and lifecycle decisions, including the scope freeze |
| [Data model](docs/data-model.md) | Source/model grain, keys, CDC and SCD2 semantics |
| [Data contracts](docs/data-contracts.md) | Source and modeled-data contracts |
| [Runbooks](docs/runbooks/) | CDC, Bronze rebuild, ClickHouse outage, Superset, and quarantine recovery |
| [Dashboard capture](docs/screenshots/README.md) | Manual screenshot procedure |
| [AGENTS.md](AGENTS.md) | Repository rules for coding agents |

## Known limitations and non-goals

Open debt is tracked in [PROGRESS.md](PROGRESS.md), not hidden by the capstone
label. Material limitations include:

- pinned MinIO image availability prevents an unconditional clean-host bootstrap
  claim;
- long-running Bronze and CDC workloads need bounded snapshot/file maintenance;
- dbt model contracts are documented and tested but are not fully enforced by
  the current adapter;
- the local benchmark fixture is evidence of the method, not a scale claim;
- this repository does not provide production HA, disaster recovery, enterprise
  security controls, or managed-cloud deployment.

The following are intentional non-goals of this completed repository:

- clickstream processing and distributed sessionization;
- platform-wide metrics dashboards and automated lineage infrastructure;
- Data Vault as a second modeling domain;
- major runtime/platform migration exercises;
- Kubernetes or cloud infrastructure deployment.

Topic-focused continuation work is intentionally separated into independent
repositories in the same profile. Those projects have their own scope and are
not unfinished OmniRetail phases.

## Maintenance policy

Normal changes are limited to documentation, bug fixes, security/dependency
maintenance, and technical-debt resolution within the delivered architecture.
Adding a new service, feature domain, or previously excluded initiative requires
a new ADR that explicitly supersedes ADR 0009.
