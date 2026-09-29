# OmniRetail Data Platform

Production-like data engineering portfolio project: an e-commerce data platform on a local Docker Compose stack. Today it ingests a PostgreSQL OLTP database, supplier files, and REST APIs into an Iceberg lakehouse (MinIO + Polaris + Trino), models the data with dbt into Silver/Gold Kimball layers and dashboard-ready marts, orchestrates everything with Airflow, publishes Gold marts to a ClickHouse serving layer, and serves BI dashboards in Apache Superset. Planned next on the same foundation: CDC via Debezium + Kafka, Spark, observability and lineage — see [ROADMAP.md](ROADMAP.md).

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

Target architecture (per [ROADMAP.md](ROADMAP.md)):

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
LATER: Airflow 3 migration, dbt v2 migration, GitLab CI, Kubernetes
```

Implemented today: PostgreSQL OLTP snapshots, supplier file ingestion, mock REST API ingestion, MinIO raw archive, Polaris + Iceberg Bronze/Silver/Gold + analytics marts (Trino + dbt), Airflow orchestration with dataset-triggered lakehouse loads, ClickHouse serving publication, Superset BI (four dashboards as code, ClickHouse + Trino paths), CI. Not yet built: Debezium CDC + Kafka, Spark, OpenLineage/Marquez, Prometheus/Grafana.

Iceberg is the analytical source of truth; the ClickHouse serving layer is derived from Iceberg Gold and always rebuildable from it.

## Project status

End-to-end today: **PostgreSQL / supplier files / mock APIs → MinIO archive → Iceberg Bronze → dbt Silver/Gold → analytics marts → ClickHouse serving copy, orchestrated by Airflow.**

Slice-level status, current focus, and deferred follow-ups live in [PROGRESS.md](PROGRESS.md); plans and acceptance criteria live in [ROADMAP.md](ROADMAP.md).

## Prerequisites

- Linux/WSL2 (target machine: Windows 11 + WSL2, 32 GB RAM)
- [uv](https://docs.astral.sh/uv/) (Python package manager)
- Python 3.12 (installed automatically by uv)
- Docker + Docker Compose
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
# --- Environment and code quality ---
make setup      # uv sync + pre-commit install
make lint       # ruff check + ruff format --check + mypy
make test       # pytest (unit tests; `unit` is an alias)

# --- Stack lifecycle ---
make up         # start the core profile and wait until healthy
make down       # stop services (named volumes are preserved)
make logs       # follow service logs
make reset      # DESTRUCTIVE: down -v, destroys all local volumes
make smoke-core # end-to-end check: Trino -> Polaris -> Iceberg -> MinIO

# --- Source data ---
make generate-oltp       # apply OLTP schema + load initial data (10k customers / 5k products / 100k orders)
make mutate-oltp         # apply a batch of random inserts/updates/deletes (EVENTS=200 by default)
make seed-supplier-files # generate deterministic vendor files and upload to landing (ROWS/SEED)

# --- Ingestion ---
make ingest-files ARGS="--source supplier-prices"  # run the file ingestion flow
make ingest-api ARGS="run --source fx-rates --date 2026-09-10"  # fetch raw API pages into archive
make ingest-api ARGS="backfill --source fx-rates --from 2026-09-01 --to 2026-09-10"  # date-range backfill

# --- Lakehouse ---
make bronze-load # load raw archive data into Iceberg Bronze (ARGS="run --source orders --date 2026-09-18" / "run-all --date ..." / "run-new")
make bronze-rebuild # DESTRUCTIVE for Bronze only: rebuild it from immutable archive with bounded Polaris recovery
make dbt-parse   # offline dbt manifest check
make dbt-build   # run dbt models + tests against the live stack (ARGS="--select staging")
make dbt-test    # run dbt tests

# --- Orchestration ---
make airflow-build     # build the custom Airflow image (omni-retail/airflow:0.1.0)
make airflow-up        # start the orchestration profile (requires the core profile up)
make airflow-down      # stop Airflow services (metadata/logs volumes preserved)
make airflow-test      # DAG import/structure tests inside the Airflow image
make airflow-dag-test ARGS="ingest_fx_api 2026-09-10"     # run one DAG for a logical date
make airflow-backfill ARGS="ingest_fx_api -s 2026-09-01 -e 2026-09-10"  # backfill a DAG

# --- Integration testing ---
make integration # integration tests against the live core stack (requires `make up`)
```

## Platform stack

The `core` Compose profile provides the lakehouse and the sources:

| Service | Image | Purpose |
|---|---|---|
| `postgres` | `postgres:16.15-alpine` | OLTP source (schema and data — see [Data sources](#data-sources-and-ingestion)) |
| `minio` + `minio-init` | pinned `minio/minio` + `mc` | S3 storage; buckets `landing`, `lakehouse`, `archive`, `rejected` |
| `polaris-postgres` | `postgres:16.15-alpine` | metadata database for Polaris (network-internal) |
| `polaris` + `polaris-bootstrap` + `polaris-init` | `apache/polaris:1.7.0` | Iceberg REST catalog (`lakehouse` catalog backed by MinIO) |
| `trino` | `trinodb/trino:483` | SQL engine; catalog `iceberg` via Polaris REST API |
| `mock-api` | `omni-retail/mock-api:0.1.0` (built locally) | deterministic external API simulator with fault injection (ADR 0002) |

The `orchestration` profile adds Airflow: a custom image `omni-retail/airflow:0.1.0`
built from `apache/airflow:2.11.2-python3.12` (webserver + scheduler, LocalExecutor)
with a dedicated metadata PostgreSQL (`airflow-postgres`) separated from the OLTP
source, so `make reset` cannot wipe Airflow state.

All host ports bind to `127.0.0.1` only: PostgreSQL `5432`, MinIO `9000`/`9001`,
mock-api `9002`, Polaris `8181`, Trino `8080`, Airflow UI `8081`. All images are
pinned; environment-driven credentials come from `.env` (see `.env.example`).

Verification:

```bash
make up          # one command to start everything (healthchecks, no sleeps)
make smoke-core  # creates iceberg.demo.healthcheck, inserts, and counts rows
make down && make up && make smoke-core  # data survives a full restart
```

Known simplification: Trino authenticates to Polaris with the bootstrap `root`
client credentials; dedicated least-privilege Polaris principals are a planned
follow-up.

## Data sources and ingestion

### OLTP source and data generator

The PostgreSQL database carries a realistic e-commerce OLTP schema
(`categories`, `products`, `customers`, `orders`, `order_items`, `payments`,
`shipments`) with PK/FK constraints, `CHECK` constraints on statuses and
amounts, and `created_at`/`updated_at` on every table. The DDL is idempotent
(`postgres/init/01_oltp_schema.sql`) and is applied automatically on a fresh
volume or explicitly via the generator.

The generator (`python -m omni_retail.generators.oltp`) is fully
deterministic: the same seed reproduces the same dataset (fixed anchor
timestamp, single `random.Random` instance).

```bash
make up                  # core stack, if not running yet
make generate-oltp       # schema + initial load: seed 42, 10k/5k/100k, ~365 days of history
make mutate-oltp EVENTS=300  # one batch: ~40% order updates, 35% inserts, 10% hard deletes, ...
```

Behavior highlights:

- initial load ages orders realistically (older orders are mostly `delivered`,
  recent ones `pending`/`paid`) with consistent payment and shipment state;
- mutations follow a strict state machine (`pending → paid → shipped →
  delivered`, cancellations/refunds update payments, `paid → shipped` creates
  an `in_transit` shipment);
- hard deletes only target `pending` orders and cascade to their items and
  payments — this provides the delete workload required for future CDC;
- re-running `initial` on a non-empty database refuses to proceed unless
  `--truncate-oltp-data` is passed (explicit, destructive).

Table documentation, grain, and source metrics: `docs/data-model.md`.

### File ingestion

External file sources flow through a durable, idempotent pipeline:

```text
supplier CSV / partner JSON / historical-orders Parquet / supplier stock XLSX
  → landing/<source>/incoming/            (drop zone)
  → landing/<source>/processing/          (transit; marker of an interrupted run)
  → validation (schema + per-row)
      ├─ ok       → archive/<source>/<yyyy>/<mm>/<dd>/<file>   (raw, unchanged)
      ├─ bad rows → rejected/<source>/<yyyy>/<mm>/<dd>/<file>.badrows.<ext>
      └─ bad file → rejected/<source>/<yyyy>/<mm>/<dd>/<file> (+ .rejection.json)
  → manifest: archive/_manifests/<source>/<batch_id>.json
  → dedup marker: archive/_dedup/<source>/<sha256>.json
```

Guarantees:

- **raw payload is preserved unchanged** — validation never rewrites the archived object;
- **idempotent re-runs** — `batch_id` is content-addressed (`<source>-<sha256[:16>]`);
  a re-upload of the same content is skipped with status `duplicate`;
- **interrupted-run recovery** — objects stranded in `processing` are reprocessed;
  `incoming` objects are deleted only after a successful archive/reject;
- **explicit quarantine** — broken files and malformed rows land in `rejected`
  with machine-readable reasons; `--fail-on-rejected` turns quarantine into a failure;
- **backfill-friendly** — archive paths use an explicit logical `--date`, never wall-clock.

Sources (schema-driven, see `omni_retail.ingestion.files.schemas`): `supplier-prices`
(CSV), `partner-products` (JSON), `historical-orders` (Parquet with typed columns),
and `supplier-stock` (XLSX edge case). All generated payloads are byte-deterministic
for the same seed — including XLSX, whose volatile Excel timestamps are normalized —
so checksum-based deduplication stays meaningful. Source contracts: `docs/data-contracts.md`;
quarantine handling: `docs/runbooks/bad-supplier-file.md`.

```bash
make up
make seed-supplier-files ROWS=500 SEED=11   # deterministic payload → landing
make ingest-files ARGS="--source supplier-prices --date 2026-09-11"
make ingest-files ARGS="--source supplier-prices --date 2026-09-11"  # again → duplicate, no double archive
```

The pipeline runs as the least-privilege MinIO user `omni-ingestion`
(rw on `landing`/`archive`/`rejected`, read-only on `lakehouse`), created
automatically by `minio-init` from `S3_ACCESS_KEY_ID`/`S3_SECRET_ACCESS_KEY`.

Note for proxied environments: if your shell sets `HTTP_PROXY`/`HTTPS_PROXY`,
add `no_proxy=127.0.0.1,localhost` so local MinIO traffic bypasses the proxy.

### API ingestion

REST sources are served by the `mock-api` container (ADR 0002): a FastAPI
service with **deterministic data** (same `MOCK_API_SEED` + logical date always
produce the same payload, so backfills are reproducible) and **fault injection**
(`?fault=429|500|timeout&fault_rate=...`; each request fingerprints faults at
most once per server lifetime, so 429 → retry → success is demonstrable).

Endpoints: `GET /api/v1/fx-rates` (offset pagination, required `date`),
`GET /api/v1/marketing/campaigns` (offset pagination, `status` filter),
`GET /api/v1/deliveries` (cursor pagination, `updated_since`), plus `/healthz`.

```text
mock-api (127.0.0.1:9002)
  → typed clients (httpx: timeout, retry + exp backoff + jitter, Retry-After,
    rate limit; 4xx != 429 fails fast as non-retryable)
  → raw page JSON: archive/api/<source>/<yyyymmdd>/page_XXXX.json (unchanged)
  → manifest:       archive/_manifests/<source>/<source>-<yyyymmdd>.json
```

Guarantees:

- **idempotent re-runs** — `batch_id = <source>-<yyyymmdd>` and page keys are
  date-addressed; a re-run of the same date overwrites the same objects;
- **backfill** — `backfill --from --to` walks logical dates sequentially,
  fails fast, and is restartable (already-done dates re-run without duplicates);
- **no wall-clock in paths or identifiers** — dates come only from explicit
  CLI arguments (the Airflow DAGs pass their logical date).

```bash
make up                                                          # includes mock-api
make ingest-api ARGS="run --source fx-rates --date 2026-09-10"
make ingest-api ARGS="run --source fx-rates --date 2026-09-10"  # again → same objects, no duplicates
make ingest-api ARGS="backfill --source marketing-campaigns --from 2026-09-09 --to 2026-09-10"
```

### PostgreSQL snapshots

`omni_retail.ingestion.postgres_snapshot` extracts each OLTP table as Parquet
with an explicit pyarrow schema:
`archive/postgres/<table>/<yyyy>/<mm>/<dd>/data.parquet`, manifest in the
shared registry (`source_kind="postgres"`), and a durable watermark
`archive/_watermarks/postgres/<table>.json`. Keyset pagination on
`(updated_at, pk)` makes same-second events safe; the watermark moves only
after a successful upload, so interruptions re-extract and overwrite the
same window. Known limitation (until CDC — see [ROADMAP.md](ROADMAP.md)):
hard deletes are invisible to snapshots, and historical backfill of past
states is impossible (a snapshot holds the current state).

## Lakehouse: Bronze → Silver → Gold → marts

### Bronze

Raw archive objects (PG snapshot Parquet, API JSON pages) are loaded into
Iceberg `bronze.*` tables partitioned by `_batch_date` via `make bronze-load`.
Loads are idempotent per (source, logical date): the day's partition is
`DELETE`d and re-filled with batched `INSERT` statements, verified against
the raw manifest row count before any DML is issued. Empty days are warnings;
schema drift and row-count mismatches fail before touching the partition.

CLI modes: `run --source --date` (one day), `run-all --date` (every source),
and `run-new`. Explicit-date modes replace their entire day partition.
`run-new` uses a deterministic `(_source_object, _source_object_row_position)`
coordinate to replace and resume only incomplete INSERT chunks, including a
partially committed watermark day; it verifies manifest count and coordinate
uniqueness at completion. `make bronze-rebuild` is the explicit destructive
entry point: it clears only the configured Bronze schema, rebuilds from the
immutable archive, and performs bounded Polaris or Trino recovery for the
known Trino 483 catalog failure and a failed Trino connection. See [the Bronze
rebuild runbook](docs/runbooks/bronze-rebuild.md).
This is the mode the `load_bronze` DAG uses. Supplier files currently stop at
the raw archive — loading them into Bronze is a tracked follow-up
([PROGRESS.md](PROGRESS.md)).

### Silver and Gold

dbt builds the analytical model over Bronze: `silver.stg_*` (typed
pass-through) and `silver.int_*` (current entity state via keyset dedup),
then the Kimball `gold` layer — `dim_date`, `dim_product`, `dim_campaign`,
SCD2 `dim_customer`, and the `fact_orders` / `fact_order_items` /
`fact_payments` / `fact_shipments` facts. Every core model carries a YAML
contract (grain, PK, measures, upstream) and is covered by built-in and
singular business tests, including the orders ↔ payments reconciliation.
Run against the live core stack with `make dbt-build`; the model reference
lives in `docs/data-model.md`.

On top of Gold, four marts provide dashboard-ready aggregates with all
financial measures normalized to EUR by `int_orders_fx` (the order
currency's FX rate at the order date):

- `mart_daily_sales` — daily GMV/revenue/margin per date × category × region;
- `mart_customer_ltv` — orders/GMV/AOV per customer with current segment;
- `mart_marketing_roi` — spend/CTR/CPC/CPM per campaign;
- `mart_delivery_performance` — transit times and status mix per carrier.

## Orchestration (Airflow)

Orchestration lives in the `orchestration` Compose profile (ADR 0003):
tasks call the ingestion and lakehouse functions in the worker process
(no shell-outs, no logic duplicated in DAGs), with dependency versions
exported from the committed `uv.lock` and the `omni_retail` package baked
into the image.

Ingestion and lakehouse transformation are chained with Airflow datasets:

```text
ingest_postgres_snapshot / ingest_fx_api / ingest_marketing_api / ingest_delivery_api
    └─ outlet: raw://<source>
          → load_bronze        (watermark-driven `run-new` over all sources)
             └─ outlet: lakehouse://bronze
                   → transform_lakehouse   (full dbt build: Silver → Gold → marts + tests)
```

Dataset URIs are logical data addresses, not S3 paths. Because supplier files
are not loaded into Bronze yet, `ingest_supplier_files` emits no dataset.
Dataset-triggered runs never rely on their own logical date (it differs from
the producer's) — the watermark sweep alone decides what to load.

| DAG | Schedule | Shape |
|---|---|---|
| `ingest_fx_api` / `ingest_marketing_api` / `ingest_delivery_api` | `@daily` | single `ingest` task, pool `mock_api`, XCom summary; outlet `raw://<source>` |
| `ingest_supplier_files` | `@daily` | 4 independent per-source tasks; param `fail_on_rejected` (default `False`) |
| `ingest_postgres_snapshot` | `@daily` | 7 independent per-table snapshot tasks; param `full_refresh` (default `False`); outlet `raw://postgres-snapshot` |
| `load_bronze` | `raw://` datasets (4 ingestion DAGs) | watermark-driven `run-new` Bronze load; outlet `lakehouse://bronze` |
| `transform_lakehouse` | `lakehouse://bronze` | full `dbt build` (models + tests) via the dbt CLI in the worker process |

Transformation SQL lives in the dbt project, never in DAGs; dbt target/log
artifacts go to a per-run temp directory (the project dir is mounted
read-only), and `max_active_runs=1` keeps concurrent dbt builds off the
single-node Trino/Polaris stack. Dataset wiring is asserted by the DAG
structure tests (`make airflow-test`); the exact task code paths — watermark
sweep, idempotent re-trigger, full build — by the live integration test
`tests/integration/test_lakehouse_orchestration.py` (`make integration`).

Shared policy: `retries=3` with exponential backoff (capped at 5 min) on top
of the http-level retries inside the API client, `execution_timeout=5min`,
`max_active_runs=1`, `catchup=False`, new DAGs start paused, and the logical
date (`ds`) is the only date input — wall-clock `now()` never appears in
paths or identifiers.

PostgreSQL snapshots (`omni_retail.ingestion.postgres_snapshot`) extract
each OLTP table as Parquet with an explicit pyarrow schema:
`archive/postgres/<table>/<yyyy>/<mm>/<dd>/data.parquet`, manifest in the
shared registry (`source_kind="postgres"`), and a durable watermark
`archive/_watermarks/postgres/<table>.json`. Keyset pagination on
`(updated_at, pk)` makes same-second events safe; the watermark moves only
after a successful upload, so interruptions re-extract and overwrite the
same window. After re-seeding the source (`make generate-oltp` with a
truncate), purge the stale watermarks first: `uv run python -m
omni_retail.ingestion.postgres_snapshot purge-watermarks` (all tables) or
`… purge-watermarks --table orders` (one table). Limitations (closed by CDC
in Phase 8): hard deletes are invisible and historical backfill is
impossible (snapshots hold current state).

```bash
make up                  # core profile first (postgres, minio, mock-api)
make airflow-up          # orchestration profile; UI at http://127.0.0.1:8081
make airflow-test        # DAG tests (DagBag) inside the image — no live services needed
make airflow-dag-test ARGS="ingest_fx_api 2026-09-10"
make airflow-dag-test ARGS="transform_lakehouse 2026-09-18"  # dataset-triggered lakehouse chain
make airflow-backfill ARGS="ingest_fx_api -s 2026-09-01 -e 2026-09-10"  # idempotent by construction
```

Manual verification of the dataset chain: `make airflow-up`, unpause the
DAGs above, trigger any ingestion DAG (or wait for its schedule), then watch
`load_bronze` and `transform_lakehouse` fire in sequence in the Datasets
view of the Airflow UI.

## Data quality and testing

- Unit tests (`make test`) stay hermetic; no network, no containers.
- dbt tests: built-in constraints plus singular business tests, including
  the orders ↔ payments reconciliation (see the Lakehouse section).
- `make integration` runs the acceptance scenarios against the live core
  stack (MinIO + mock-api + Trino), gated by `OMNI_INTEGRATION=1`.
  The suite is safe on a long-lived stack: deterministic object coordinates
  are borrowed through exact-key rollback journals, a session checksum
  invariant verifies the complete archive byte-for-byte, and lakehouse tests
  use UUID-prefixed disposable Bronze/Silver/Gold/analytics schemas. Shared
  production schemas are never reset and no Bronze reload is required after
  tests:
  - re-running the same file batch archives exactly once (content-addressed dedup);
  - a corrupted file is quarantined with a machine-readable `.rejection.json`;
  - Parquet and XLSX sources flow end-to-end through real object storage;
  - an injected HTTP 429 is retried and succeeds;
  - an API backfill over a date range is idempotent (deterministic page keys);
  - a PostgreSQL snapshot lifecycle: full extract → parquet + manifest + watermark,
    re-run with an empty window (no duplicates), incremental extract after a
    controlled mutation, and `full_refresh` rebase;
  - a dataset-triggered lakehouse orchestration run: seeded raw data →
    watermark-swept Bronze load → full dbt build, with an idempotent
    re-trigger (no duplicates).

## Project layout

```text
src/omni_retail/    Python package: ingestion (files, api, postgres_snapshot),
                   generators, lakehouse (bronze), serving (clickhouse publisher)
dbt/               dbt project: staging → intermediate → core → marts
airflow/           DAGs, dataset definitions, shared policy and runners
postgres/          OLTP schema DDL (idempotent, applied on first volume init)
infrastructure/    custom Dockerfiles, init/bootstrap scripts, smoke and DAG-test scripts
trino/             Trino configuration
tests/             unit tests + opt-in integration tests (OMNI_INTEGRATION=1)
docs/              data model, data contracts, ADRs, runbooks, agent guides
```

## Documentation map

Each documentation artifact has a single role:

| Document | Owns |
|---|---|
| [AGENTS.md](AGENTS.md) | Core rules for AI coding agents: invariants, workflow, Definition of Done, prohibitions, phase gates; routes to the topical guides |
| [docs/agent/](docs/agent/) | Normative topical guides (ingestion, dbt modeling, lakehouse, Airflow, reliability, testing, …) — read on demand via the AGENTS.md routing table |
| [docs/adr/](docs/adr/README.md) | Architecture Decision Records (index, template); significant decisions only |
| [ROADMAP.md](ROADMAP.md) | Phases, scope, acceptance criteria, target architecture, repository structure |
| [PROGRESS.md](PROGRESS.md) | Current state only: phase/slice status, current focus, deferred follow-ups |
| `docs/plans/active.md` (when present) | Execution detail of the single active task (session-resume checklist; removed when the task closes) |
| [docs/data-model.md](docs/data-model.md) | Table documentation, grain, source metrics |
| [docs/data-contracts.md](docs/data-contracts.md) | Source data contracts |
| [docs/runbooks/](docs/runbooks/) | Operational failure runbooks |

Precedence on conflict: explicit request > AGENTS.md (core + guides) > accepted
ADRs > ROADMAP.md > existing conventions. The audit trail of *when* things
changed is git history — these documents describe only the current state and
the rules that govern changing it.

## ClickHouse serving layer

Gold marts are published from Iceberg to ClickHouse as a rebuildable,
read-only-for-BI serving copy ([ADR 0004](docs/adr/0004-clickhouse-serving-publication.md)):
the publisher reads a full mart snapshot through Trino, inserts it into a
`_staging` twin, and atomically swaps the pair with `EXCHANGE TABLES` — BI
never sees a partial publish, and re-running a publish can never duplicate
rows. Routine refresh, retry after failure, and rebuild share this one code
path.

```bash
make bi-up            # profile bi: ClickHouse + one-shot versioned migrations
make serving-publish ARGS="--mart mart_daily_sales"
make serving-rebuild   # every mart, drop-safe: recreate DDL + republish from Gold
make serving-rebuild ARGS="--mart mart_daily_sales"   # ...or a single mart
make serving-benchmark                            # Trino vs ClickHouse report
```

Schema changes are versioned migrations under `clickhouse/migrations/`,
applied by the `clickhouse-init` container and tracked in the
`analytics.schema_migrations` ledger. Service accounts follow least
privilege: `omni_publisher` (write grants on `analytics.*` only) and
`superset_reader` (SELECT-only, reserved for the Superset BI layer). Only the
HTTP interface is published to the host (`127.0.0.1:8123`); the native
port stays docker-network-only. Idempotency, rebuild-from-Gold parity,
and reader permissions are asserted by
`tests/integration/test_serving_publication.py`
(`make up && make bi-up && make integration`). After a successful
`transform_lakehouse` run, the dataset-triggered `publish_serving` DAG
rebuilds all four marts from Gold. Compare the representative query with
`make serving-benchmark`; the report is written to
`docs/benchmarks/phase6-trino-vs-clickhouse.md`. Outage recovery is documented
in [`docs/runbooks/clickhouse-outage.md`](docs/runbooks/clickhouse-outage.md).

## Superset BI

The `bi` profile also runs Apache Superset ([ADR 0005](docs/adr/0005-superset-deployment.md))
as a custom pinned image (`omni-retail/superset:0.1.0`) with a dedicated
metadata PostgreSQL and an idempotent one-shot bootstrap: schema migrations,
admin user, the two connections (`ClickHouse analytics` over the docker
network with the read-only `superset_reader` account, and `Trino iceberg`
for ad-hoc exploration), and an import of the BI assets committed under
[`superset/`](superset/). Web UI: `http://127.0.0.1:8088` (loopback only).

BI assets are code: the four mart datasets with their documented metrics and
four dashboards — **Sales** (KPI tiles, revenue/margin and AOV trends,
category/region breakdowns), **Executive** (monthly revenue/margin/orders
trends, GMV, region/category mix, delivery operations), **Customer**
(acquisition cohorts by first-order month, new-vs-repeat composition, repeat
rate, LTV by segment/region, AOV distribution, top customers) and
**Marketing** (spend vs budget, CTR, CPC/CPM, budget utilization, campaign
scorecard) — live in `superset/assets/*.zip` and are re-imported on every
`make bi-up`: a clean clone converges to the same BI state from the
repository plus `.env`, with no manual UI steps. Bundle exports are
sanitized before committing (`infrastructure/scripts/superset_bundle_sanitize.py`):
connection credentials never enter Git. The round-trip loop and operational
procedures live in [`docs/runbooks/superset.md`](docs/runbooks/superset.md);
bootstrap invariants, a Superset-vs-ClickHouse canary reconciliation and the
Trino SQL Lab ad-hoc path are asserted by `tests/integration/test_superset_bootstrap.py`.

### Dashboard gallery

Screenshots of every delivered dashboard belong in the README (ROADMAP
Phase 7 acceptance); capture steps live in
[`docs/screenshots/README.md`](docs/screenshots/README.md):

| Dashboard | Screenshot |
| --- | --- |
| Sales | `docs/screenshots/sales-dashboard.png` |
| Executive | `docs/screenshots/executive-dashboard.png` |
| Customer | `docs/screenshots/customer-dashboard.png` |
| Marketing | `docs/screenshots/marketing-dashboard.png` |

The Funnel dashboard and conversion/ROAS metrics wait for Phase 9
clickstream data (see `PROGRESS.md`).

### Ad-hoc exploration: Superset → Trino → Iceberg

Dashboards always read the published ClickHouse marts (low latency,
read-only serving copy). For ad-hoc exploration over the full lakehouse —
Bronze/Silver internals, Gold history, or queries the marts do not cover —
switch the SQL Lab database to **Trino iceberg** and query Iceberg directly
(`SELECT count(*) FROM gold.fact_orders`). Prefer ClickHouse for repeated
dashboard-style aggregation; prefer the Trino path when you need the whole
modeled history or non-mart grains. The path is exercised by
`tests/integration/test_superset_bootstrap.py::test_sqllab_trino_adhoc_path`.

## CI

GitHub Actions runs on every push and pull request: ruff check, ruff format, mypy (non-blocking at bootstrap), pytest, `docker compose config` validation, plus an `airflow` job that builds the custom image and runs the DAG test harness (pytest + `airflow dags list-import-errors`) inside it.
