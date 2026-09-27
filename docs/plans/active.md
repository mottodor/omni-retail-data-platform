# Active plan — Phase 7: Apache Superset (sliced)

Task: implement Phase 7 of `ROADMAP.md` as three vertical slices. This plan
owns the slice breakdown and the checklists. Division of labor (AGENTS §41.3):
acceptance criteria live in `ROADMAP.md`, architectural decisions in
`docs/adr/`, status in `PROGRESS.md`; this file owns execution detail. Delete
this file in the closing commit of the phase.

Created: 2026-09-27 (planning session).

## Goal

A reproducible BI layer on top of the Phase 6 serving stack, with no external
network access and no new exposure of ClickHouse:

- Superset -> ClickHouse (`superset_reader`, SELECT-only) is the primary
  dashboard source; queries hit the published marts, not Gold (guide §26/§27);
- Superset -> Trino is the secondary ad-hoc exploration path;
- connections and dashboards are reproducible from a clean clone: service
  accounts and connection secrets come from `.env`, dashboard/dataset metadata
  is exported to and imported from this repository (ROADMAP acceptance);
- screenshots of every delivered dashboard land in the README (ROADMAP
  acceptance).

## Context to read first (session resume)

1. `AGENTS.md` core + `docs/agent/serving-bi.md` (§25–27) +
   `docs/agent/engineering-practices.md` (§40–45, §50) +
   `docs/agent/platform-ops.md` (compose/Makefile rules).
2. `ROADMAP.md` — Phase 7 section + §3 (stack) + §4 (profiles: `bi` =
   ClickHouse + Superset) + §5 (repo layout: top-level `superset/`).
3. `PROGRESS.md` — current focus and deferred items.
4. `docs/adr/0001-project-architecture.md`, `docs/adr/0003-airflow-deployment.md`
   (custom-image conventions reused here), `docs/adr/0004-clickhouse-serving-publication.md`.
5. Implementation neighbors to mirror:
   - `docker-compose.yml` — `clickhouse`/`clickhouse-init` and
     `airflow-postgres`/`airflow-init` service patterns;
   - `infrastructure/airflow/Dockerfile` — pinned custom image from the
     committed lockfile;
   - `infrastructure/scripts/clickhouse_init.sh` / `polaris_init.py` —
     one-shot idempotent init services;
   - `tests/test_compose_guards.py` — compose quality guards to extend;
   - `tests/integration/test_serving_publication.py` — live-`bi`-profile test
     conventions.

## Facts established at planning time

- Four marts are published and reconciled in ClickHouse `analytics`
  (Phase 6): `mart_daily_sales`, `mart_customer_ltv`, `mart_marketing_roi`,
  `mart_delivery_performance`. `superset_reader` (SELECT-only) already exists
  (migration `0002_users.sql`), credentials in `.env`
  (`CLICKHOUSE_READER_USER`/`CLICKHOUSE_READER_PASSWORD`).
- **No funnel mart exists** — clickstream arrives in Phase 9; the Funnel
  dashboard is deferred with it (already recorded in `PROGRESS.md` deferred
  list). ROADMAP Phase 7 wording is satisfied by the 4 deliverable dashboards.
- **ROAS/CAC are not computable** from current data (no revenue attribution;
  documented in `dbt/models/marts/schema.yml`). The Marketing dashboard shows
  impressions/clicks/CTR/CPC/CPM/budget utilization; ROAS arrives with
  clickstream attribution (Phase 9). Same for Executive "conversion" (needs
  visits) — deferred to Phase 9.
- Customer data supports new-vs-returning (by `first_order_date` cohort),
  LTV by segment/region, repeat rate; classic retention curves/RFM need richer
  event history — deliver what the mart supports, document the limitation.
- Superset engine spec for ClickHouse (verified against upstream docs):
  `clickhousedb://user:password@host:8123/database`, recommended driver
  `clickhouse-connect` — matches the already-pinned platform versions
  `clickhouse-connect==1.9.0`, `trino==0.339.0` in `uv.lock`. The official
  `apache/superset` image ships **without** these drivers → custom image
  (ADR 0003 pattern), versions exported from the committed lockfile.
- Trino local deployment has no auth; the ad-hoc connection is
  `trino://<user>@trino:8080/iceberg` over the docker network. Documented as
  a local-only posture (Trino is loopback-published already, unchanged).
- Host port 8088 is free (taken: 5432, 8080, 8081, 8123, 8181, 9000–9002);
  Superset web binds `127.0.0.1:8088` only. ClickHouse stays reachable to
  Superset via docker-network hostname `clickhouse` — no new ClickHouse
  exposure (ROADMAP acceptance).
- Superset metadata DB: dedicated `superset-postgres` (mirrors
  `airflow-postgres`), profile `bi`, named volume.
- Dashboard YAML exports contain no credentials → safe to commit; database
  connections are the only secret-bearing objects and are created by the init
  script from `.env`, never exported (AGENTS §10).

## Decision to ratify first — ADR 0005 (Superset deployment and BI-as-code)

Recommended (to be confirmed/adjusted when writing the ADR as step 1.1):

- **Deployment**: custom image `omni-retail/superset:0.1.0` built from pinned
  `apache/superset:5.0.0` (latest stable line; 6.x is RC) with
  `clickhouse-connect==1.9.0` + `trino==0.339.0` exported from `uv.lock`;
  three compose services in profile `bi` — `superset-postgres`,
  `superset` (config via mounted `superset/superset_config.py`,
  `SUPERSET_CONFIG_PATH`), `superset-init` (one-shot, healthcheck-gated).
- **BI-as-code**: an idempotent Python init script runs `superset db upgrade`,
  upserts the admin user and the two database connections from env vars, then
  imports datasets/dashboards from the committed `superset/` export. Content
  built in the UI is round-tripped back with `superset export` and committed
  — the repo stays the source of truth for BI assets.
- **Secrets**: `SUPERSET_SECRET_KEY`, `SUPERSET_ADMIN_USER/PASSWORD`,
  `SUPERSET_POSTGRES_USER/PASSWORD/DB` in `.env` (documented in
  `.env.example`); no secrets in YAML exports.
- **Alternatives for the ADR**: manual UI setup on top of the stock image
  (not reproducible from a clean clone — violates the acceptance criteria);
  SQLite metadata file in a volume (not production-like, fragile upgrades);
  `preset-io/sup` CLI for asset sync (extra tool, pulls against the
  no-network constraint); env-var-only bootstrap without committed assets
  (dashboards would be manual-only — AGENTS §48 prohibits manual-only infra
  steps without documentation).

---

## Steps

Vertical slices; each closes with its own validation and PROGRESS update.

### Slice 1 — Superset platform slice (service + connections, no dashboards)

- [x] 1.1 Write and accept `docs/adr/0005-superset-deployment.md`.
- [x] 1.2 Custom image: `infrastructure/superset/Dockerfile` — pinned base
  `apache/superset:5.0.0`, pinned `uv` binary, `uv export --frozen` +
  install of only the two BI drivers (full requirements export is not needed
  — the image already carries Superset's own deps; document the reason),
  non-root user preserved. Compose: `omni-retail/superset:0.1.0`.
- [x] 1.3 Compose services (profile `bi`):
  - `superset-postgres`: postgres 16.15-alpine, named volume, healthcheck
    (mirror `airflow-postgres`);
  - `superset`: depends on healthy metadata DB; ports
    `127.0.0.1:${SUPERSET_PORT:-8088}:8088`; healthcheck on `/health`;
    mounts `./superset:/app/superset_home/repo:ro` for config + assets;
    `SUPERSET_CONFIG_PATH` pointing at the mounted config;
  - `superset-init`: one-shot, `depends_on: superset: service_healthy`,
    runs `infrastructure/scripts/superset_init.py` with env-provided
    credentials; idempotent (safe on every `make bi-up`).
- [x] 1.4 `superset/superset_config.py`: metadata URI to `superset-postgres`,
  `SECRET_KEY` from env (fail loudly if unset), examples off, mapbox off,
  `TALISMAN_ENABLED = False` (local loopback only — documented), feature
  flags conservative.
- [x] 1.5 `infrastructure/scripts/superset_init.py` (typed, structured logs):
  `db upgrade` → admin upsert (no duplicate on re-run) → upsert Database
  connection "ClickHouse analytics" (`clickhousedb://…@clickhouse:8123/analytics`,
  driver `connect`, `superset_reader` creds from env, impersonate off) →
  upsert "Trino iceberg" (`trino://omni_superset@trino:8080/iceberg`) →
  import assets from the mounted `superset/assets/` if present (empty tree
  in this slice — the import step itself is tested).
- [x] 1.6 `.env.example`: SUPERSET_* block (port, secret key, admin
  user/password, metadata DB creds) with comments mirroring Phase 6 style.
- [x] 1.7 Makefile: `bi-up` builds the superset image alongside ClickHouse;
  `bi-down` unchanged semantics (superset metadata volume preserved);
  help text updated.
- [x] 1.8 Tests:
  - extend `tests/test_compose_guards.py`: superset image pinned + built,
    healthcheck present, loopback-only ports, env vars documented;
  - unit tests for the init script's decision logic (connection upsert
    semantics) against fakes, mirroring `tests/unit/serving/` conventions;
  - integration `tests/integration/test_superset_bootstrap.py`
    (`make up && make bi-up` required): `/health` returns OK; the two
    database connections exist with correct hostnames and the reader
    account; ClickHouse connection test-query succeeds through
    `superset_reader`.
- [x] 1.9 Validation: `make lint`, `make test`, `docker compose config`,
  `make bi-up` from clean volumes; close with PROGRESS focus update.

#### Slice 1 implementation notes (facts discovered, binding for slices 2–3)

- **Superset 5.0.0 runtime facts** (verified live in the image):
  - `python` on PATH is the `/app/.venv` interpreter; `uv pip install
    --system` targets `/usr/local` instead — the Dockerfile passes
    `--python /app/.venv/bin/python` explicitly.
  - The WSGI target is the factory `superset.app:create_app()` (`FLASK_APP`),
    not `superset.app:app`; the compose service uses the image's own CMD
    (`run-server.sh`, env-tunable), no hand-written gunicorn command.
  - The security manager has no `bcrypt` attribute: password hashing is
    `werkzeug.security` (`check_password_hash` / FAB's `reset_password`).
  - The base image ships `zstandard==0.23.0` (excluded from the driver
    whitelist) but no postgres driver — `psycopg2-binary==2.9.13` was added
    to `pyproject.toml`/`uv.lock` (metadata-DB driver only; the platform
    itself stays on psycopg v3).
- **Docker Compose v5.5.1**: `up --wait` treats exited-0 one-shot containers
  as a wait failure (this also affects the Phase 6 shape of `bi-up`).
  `make bi-up` is now two-phase: `up -d --wait --build clickhouse
  superset-postgres superset` (services targeted by name, no `--profile`,
  so one-shots stay out of the wait set), then `docker compose run --rm`
  for each one-shot initializer (blocking, exit code propagates).
- **Superset REST API**: list/detail `/api/v1/database/` mask/hide the
  SQLAlchemy URI; `GET /api/v1/database/<pk>/connection` returns it — the
  integration test uses that endpoint.
- Bootstrap idempotency verified live twice: re-run over existing state
  prints `admin=skip clickhouse_connection=skip trino_connection=skip
  assets=skip`; from a removed metadata volume it prints `*=create` and
  the integration suite passes afterwards.

### Slice 2 — BI-as-code loop + Sales dashboard (first dashboard end-to-end)

- [ ] 2.1 Datasets as code: physical datasets on the ClickHouse connection
  for `analytics.mart_daily_sales` (+ the other three while at it — one
  export, no extra mechanism), columns documented, metrics defined where the
  marts imply them (revenue_eur sum, margin_eur sum, orders_count,
  AOV = revenue/orders as a Superset metric).
- [ ] 2.2 Sales dashboard (ROADMAP: revenue by date/category/region, top
  products — see facts above): time-series revenue/margin/orders, breakdown
  by category and region, AOV trend, items sold; built first in the UI,
  exported, then imported by `superset-init` and re-verified.
- [ ] 2.3 Round-trip documented: build in UI → `superset export` → commit
  YAML under `superset/assets/` → `make bi-down && make bi-up` re-imports;
  record the exact commands in the runbook stub (slice 3 completes it).
- [ ] 2.4 Screenshot of the Sales dashboard → `docs/screenshots/` +
  README BI section stub.
- [ ] 2.5 Extend the integration test: imported datasets present; a canary
  chart query returns the same totals as a direct ClickHouse/Trino query
  (lightweight echo of the Phase 6 reconciliation idea, one number).

### Slice 3 — Remaining dashboards, Trino ad-hoc path, docs, close-out

- [ ] 3.1 Executive dashboard: GMV, revenue, margin, orders, AOV tiles and
  trends from `mart_daily_sales`; optional delivery ops tiles from
  `mart_delivery_performance` (data already served — decide by chart value,
  not novelty); "conversion deferred to Phase 9" note in README, not on the
  dashboard.
- [ ] 3.2 Customer dashboard from `mart_customer_ltv`: new vs returning by
  first-order cohort month, LTV by segment/region, repeat-rate KPI,
  avg_order_value distribution; retention/RFM documented as data-limited.
- [ ] 3.3 Marketing dashboard from `mart_marketing_roi`: impressions, clicks,
  CTR, CPC/CPM, budget utilization by channel/campaign; ROAS/CAC limitation
  documented (no attribution in source data).
- [ ] 3.4 Trino ad-hoc path: verify SQL Lab works against
  `trino://…/iceberg` (connection from slice 1); README exploration section
  ("Superset -> Trino -> Iceberg", when to prefer it over ClickHouse).
- [ ] 3.5 `docs/runbooks/superset.md`: up/down, re-import, re-export,
  secret rotation, common failures (metadata DB down, reader password
  mismatch, import checksum mismatch).
- [ ] 3.6 README: all dashboard screenshots, quickstart (core → bi profile),
  BI architecture line (AGENTS §3.3 paths).
- [ ] 3.7 Close the phase: PROGRESS — Phase 7 done, slice table, focus →
  Phase 8; deferred entries for Funnel dashboard + conversion/ROAS metrics
  (Phase 9); delete `docs/plans/active.md` in the closing commit.

## Validation

Per slice: `make lint`, `make test`, `docker compose config`; with the stack
up: `make up && make bi-up`, then
`uv run pytest tests/integration/test_superset_bootstrap.py -m integration`
(markers per `tests/integration/conftest.py`). Phase-level acceptance
mapping (ROADMAP Phase 7): docker-network hostname — slice 1; no ClickHouse
exposure for dashboards — slice 1 (loopback 8123 remains CLI/test-only);
metadata export in repo — slice 2; screenshots in README — slices 2–3.

## Scope fence

In scope: the three compose services, the custom image, config/init scripts,
`superset/` assets, tests, docs, `.env.example`, Makefile wiring.

Out of scope (do not do): new dbt marts or mart schema changes; funnel /
conversion / ROAS metrics (Phase 9 data); Airflow changes (publication is
already dataset-triggered); Superset alerts/reports (needs email/redis
infra); row-level security / multi-tenant roles (single-admin local BI);
embedding/auth proxies; any ClickHouse engine or publication changes; CI
workflows beyond existing guards (Phase 13 owns CI v2).
