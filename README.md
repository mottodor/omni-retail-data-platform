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

- [x] Phase 0 — repository bootstrap and engineering standards
- [x] Phase 1 — core infrastructure (PostgreSQL, MinIO, Polaris, Trino, Iceberg)
- [x] Phase 2 — OLTP model and deterministic data generator
- [x] Phase 3 — batch ingestion (CSV/JSON/Parquet/XLSX files, mock API service, retries, backfill, live integration tests)
- [ ] Phase 4+ — see `ROADMAP.md`

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
make setup      # uv sync + pre-commit install
make lint       # ruff check + ruff format --check + mypy
make test       # pytest
make unit       # alias for make test
make up         # start the core profile and wait until healthy
make down       # stop services (named volumes are preserved)
make logs       # follow service logs
make reset      # DESTRUCTIVE: down -v, destroys all local volumes
make smoke-core # end-to-end check: Trino -> Polaris -> Iceberg -> MinIO
make generate-oltp # apply OLTP schema + load initial data (10k customers / 5k products / 100k orders)
make mutate-oltp   # apply a batch of random inserts/updates/deletes (EVENTS=200 by default)
make seed-supplier-files  # generate deterministic vendor files and upload to landing (ROWS/SEED)
make ingest-files ARGS="--source supplier-prices"  # run the file ingestion flow
make ingest-api ARGS="run --source fx-rates --date 2026-09-10"  # fetch raw API pages into archive
make ingest-api ARGS="backfill --source fx-rates --from 2026-09-01 --to 2026-09-10"  # date-range backfill
make integration  # integration tests against the live core stack (requires `make up`)
```

## Core infrastructure (Phase 1)

The `core` Compose profile provides the minimal working lakehouse:

| Service | Image | Purpose |
|---|---|---|
| `postgres` | `postgres:16.15-alpine` | OLTP source (schema and data from Phase 2, see below) |
| `minio` + `minio-init` | pinned `minio/minio` + `mc` | S3 storage; buckets `landing`, `lakehouse`, `archive`, `rejected` |
| `polaris-postgres` | `postgres:16.15-alpine` | metadata database for Polaris (network-internal) |
| `polaris` + `polaris-bootstrap` | `apache/polaris:1.7.0` | Iceberg REST catalog (`lakehouse` catalog backed by MinIO) |
| `trino` | `trinodb/trino:483` | SQL engine; catalog `iceberg` via Polaris REST API |
| `mock-api` | `omni-retail/mock-api:0.1.0` (built locally) | deterministic external API simulator with fault injection (ADR 0002) |

All host ports bind to `127.0.0.1` only: PostgreSQL `5432`, MinIO `9000`/`9001`,
Polaris `8181`, Trino `8080`. All images are pinned; environment-driven
credentials come from `.env` (see `.env.example`).

Verification:

```bash
make up          # one command to start everything (healthchecks, no sleeps)
make smoke-core  # creates iceberg.demo.healthcheck, inserts, and counts rows
make down && make up && make smoke-core  # data survives a full restart
```

Known simplification: Trino authenticates to Polaris with the bootstrap `root`
client credentials; dedicated least-privilege Polaris principals are planned
with the ingestion phases.

## OLTP source and data generator (Phase 2)

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

## Batch ingestion (Phase 3)

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

## API ingestion (Phase 3)

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
  CLI arguments (Airflow will pass logical dates in Phase 4).

```bash
make up                                                          # includes mock-api
make ingest-api ARGS="run --source fx-rates --date 2026-09-10"
make ingest-api ARGS="run --source fx-rates --date 2026-09-10"  # again → same objects, no duplicates
make ingest-api ARGS="backfill --source marketing-campaigns --from 2026-09-09 --to 2026-09-10"
```

## Integration tests (Phase 3)

`make integration` runs the Phase 3 acceptance scenarios against the live core
stack (MinIO + mock-api), gated by `OMNI_INTEGRATION=1` so plain `make test`
stays hermetic:

- re-running the same file batch archives exactly once (content-addressed dedup);
- a corrupted file is quarantined with a machine-readable `.rejection.json`;
- Parquet and XLSX sources flow end-to-end through real object storage;
- an injected HTTP 429 is retried and succeeds;
- an API backfill over a date range is idempotent (deterministic page keys).

## CI

GitHub Actions runs on every push and pull request: ruff check, ruff format, mypy (non-blocking at bootstrap), pytest, and `docker compose config` validation.
