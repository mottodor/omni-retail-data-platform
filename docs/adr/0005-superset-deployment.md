# ADR 0005 — Superset deployment and BI-as-code bootstrap

## Title

Deploy Apache Superset in the `bi` Compose profile as a custom pinned image with a dedicated metadata PostgreSQL, an idempotent one-shot bootstrap (schema, admin, two database connections, asset import), and repository-committed BI assets — no new ClickHouse exposure, no external network access.

## Status

Accepted.

## Context

Phase 7 (`ROADMAP.md`) adds the BI layer: Superset -> ClickHouse is the
primary dashboard source, Superset -> Trino the secondary ad-hoc path
(AGENTS.md §3.3, guide §27). Facts that constrain the decision:

- The serving stack from Phase 6 (ADR 0004) already publishes the four Gold
  marts into ClickHouse `analytics` and provides the least-privilege
  `superset_reader` account (SELECT-only). Credentials live in `.env`
  (`CLICKHOUSE_READER_USER`/`_PASSWORD`).
- ROADMAP Phase 7 acceptance requires that connections and dashboards are
  reproducible from a clean clone (metadata export in the repository) and
  that dashboards do not require publishing ClickHouse beyond its existing
  loopback-only HTTP port.
- The official `apache/superset` image ships **without** the two drivers this
  platform needs: `clickhouse-connect` (the Superset-recommended ClickHouse
  connector — engine `clickhousedb`, default driver `connect`, HTTP port
  8123) and `trino` (SQLAlchemy dialect for the exploration path). It also
  defaults its metadata DB to sqlite and ships no PostgreSQL driver, so a
  dedicated metadata PostgreSQL additionally requires `psycopg2-binary`.
  All three are pinned in the committed `uv.lock` (`clickhouse-connect==1.9.0`,
  `trino==0.339.0`, `psycopg2-binary==2.9.13`).
- ADR 0003 established the custom-image convention: pinned base image, `uv`
  binary copied from a pinned tag, dependency versions exported from the
  committed lockfile without fresh resolution.
- The `bi` profile already owns the serving concern; host port 8088 is free
  (taken: 5432, 8080, 8081, 8123, 8181, 9000–9002).
- Trino local deployment has no authentication; the ad-hoc connection
  `trino://omni_superset@trino:8080/iceberg` works only inside the Docker
  network and is a documented local-only posture (Trino is already published
  loopback-only, unchanged by this ADR).

## Decision

### Deployment model

- **Custom image** `omni-retail/superset:0.1.0` built from pinned
  `apache/superset:5.0.0` (latest stable line; 6.x is RC) via
  `infrastructure/superset/Dockerfile`, mirroring ADR 0003: pinned `uv`
  binary, versions exported from the committed `uv.lock`. Only the two BI
  drivers plus their transitive dependencies *not already present* in the
  base image are installed, with `--no-deps`, from the frozen export — a full
  platform export would drag dbt/boto3/psycopg into the image and risk
  clobbering Superset's own pins; shared transitives (requests, urllib3,
  certifi, …) are provided by the base image itself. The install ends with a
  build-time smoke import so a missing transitive fails the build, not
  runtime.
- **Three compose services in profile `bi`** (the profile from ADR 0004
  gains its consumer):
  - `superset-postgres` — postgres 16.15-alpine, named volume
    `superset-metadata-data`, healthcheck; mirrors `airflow-postgres`;
  - `superset` — depends on a healthy metadata DB; config via mounted
    `superset/superset_config.py` and `SUPERSET_CONFIG_PATH`; web port
    `127.0.0.1:${SUPERSET_PORT:-8088}:8088` (loopback only); healthcheck on
    `/health`; single gunicorn worker with 8 gthread threads (workstation
    resource budget, guide §12.2);
  - `superset-init` — one-shot, gated on `superset: service_healthy`,
    idempotent on every `make bi-up`.
- **Connections use Docker-network hostnames** (`clickhouse:8123`,
  `trino:8080`); ClickHouse keeps exactly its existing loopback-only
  `8123` mapping for host-side CLI/tests — no new exposure for dashboards.

### Bootstrap (one-shot, idempotent)

`infrastructure/scripts/superset_init.py` (stdlib + subprocess at the edges,
Superset Python API inside the app context):

1. `superset db upgrade` (schema migrations) — safe to re-run;
2. `superset init` (default roles/permissions) — idempotent;
3. admin user upsert — create if missing, update the password if it changed
   in `.env` (supports rotation), never duplicate;
4. Database connection upserts, keyed by `database_name`:
   - "ClickHouse analytics" — `clickhousedb://superset_reader:…@clickhouse:8123/analytics`
     (driver `connect` is the engine default), impersonation off;
   - "Trino iceberg" — `trino://omni_superset@trino:8080/iceberg`;
   existing connections are updated in place when the URI changed;
5. asset import from the mounted `superset/assets/` tree — skipped with a
   log line when the tree is empty (Phase 7 slice 1 ships an empty tree and
   tests the skip; slices 2–3 fill it and exercise the import).

### BI-as-code loop

Dashboard/dataset metadata built in the UI is exported (ZIP bundle via the
Superset import/export v1 format) and committed under `superset/assets/`;
`superset-init` re-imports it on every bootstrap, so the repository stays the
source of truth for BI assets and a clean clone converges to the same BI
state. Connection objects are the only secret-bearing entities and are never
exported — they are always created from `.env` by the init script
(AGENTS.md §10).

### Secrets and configuration

`SUPERSET_SECRET_KEY` (required — the config fails loudly if unset),
`SUPERSET_ADMIN_USER`/`_PASSWORD`, `SUPERSET_POSTGRES_USER`/`_PASSWORD`/`_DB`,
`SUPERSET_PORT` live in `.env`, documented in `.env.example`. The metadata
URI is built in `superset/superset_config.py` with password URL-quoting.
Talisman stays disabled (loopback-only deployment, documented); examples are
never loaded; MapBox stays off.

## Alternatives considered

- **Manual UI setup on the stock image:** rejected — not reproducible from a
  clean clone, violating the ROADMAP acceptance criteria and AGENTS §48
  (manual-only infrastructure steps without documentation).
- **SQLite metadata file in a volume:** rejected — not production-like,
  fragile across Superset upgrades, and it would diverge from the dedicated
  metadata-DB pattern already used for Airflow (ADR 0003).
- **`preset-io/sup` CLI for asset sync:** rejected — an extra tool with its
  own dependency surface and network pulls, against the no-external-network
  constraint; the built-in import/export format covers the need.
- **Env-var-only bootstrap without committed assets:** rejected — dashboards
  would be manual-only artifacts, not reproducible.
- **Installing the full platform dependency export into the image:** rejected
  — the base image already pins Superset's own dependency tree; overlaying
  dbt/boto3/psycopg pins on it risks breaking the web app for zero benefit.
- **Running `superset-init` logic as an ad-hoc `docker compose exec` one-liner
  chain:** rejected — the official pattern of an init container is
  declarative, gated on health, and re-runnable via `make bi-up` (ADR 0003
  precedent).

## Consequences

- Positive:
  - a clean clone + `make up && make bi-up` converges to a fully configured
    BI layer (schema, admin, connections, dashboards) with no manual steps;
  - BI assets live in Git with reviewable diffs; the metadata database is
    rebuildable from the repository plus `.env`;
  - ClickHouse exposure does not grow; Superset traffic stays on the Docker
    network with the read-only `superset_reader` account (guide §26.4);
  - Superset's dependency tree is not disturbed — only additive driver
    installs from the frozen lockfile.
- Negative / accepted costs:
  - the import/export v1 bundle is an internal format that can change between
    Superset major versions — upgrades require a re-export (documented in the
    Phase 7 runbook);
  - `superset-init` shares the custom image with the web service, so image
    size (~2 GB) is paid once for both — acceptable for a local platform;
  - single gunicorn worker without Celery/Redis: alerts/reports and
    long-running async queries are out of scope (single-admin local BI);
  - the Trino connection has no authentication — a documented local-only
    posture, consistent with the existing Trino deployment.
- Measurability: the init script logs each step's decision
  (created/updated/skipped/imported counts), so a bootstrap is auditable from
  logs alone.

## Rollback / migration considerations

- Removing Superset entirely: drop the three `bi`-profile services, the
  `superset/` tree, the Dockerfile, the init script, the Makefile wiring and
  the `SUPERSET_*` env block; ClickHouse serving data is untouched (Superset
  holds no business data).
- Metadata DB corruption: `make bi-down`, remove the
  `superset-metadata-data` volume, `make bi-up` — schema, admin, connections
  and assets are re-created by the init script from the repository + `.env`.
- Superset version upgrade: pin the new base image, rebuild, `superset db
  upgrade` re-runs, then re-export assets from the upgraded instance and
  commit the refreshed bundles.
- Moving BI assets to a different sync tool later: the committed bundle
  format is tool-agnostic ZIP/YAML; only the import step in the init script
  changes.
