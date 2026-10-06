# Superset BI layer — runbook

Phase 7 (ADR 0005). Operational procedures for the Superset BI layer:
lifecycle, the BI-as-code round-trip, and common failures. The `bi` profile
also owns ClickHouse — see [`clickhouse-outage.md`](clickhouse-outage.md)
for serving-layer incidents.

## Lifecycle

```bash
make up        # core stack first (Trino, ClickHouse dependencies, sources)
make bi-up     # ClickHouse + Superset: builds the custom image, waits for
               # health, then runs the idempotent one-shot initializers
               # (clickhouse-init, superset-init)
make bi-down   # stop the bi profile (metadata and ClickHouse volumes preserved)
```

`superset-init` is idempotent and safe on every `make bi-up`. Its summary
line is the audit record of what happened:

```text
superset_init: bootstrap complete admin=skip clickhouse_connection=skip trino_connection=skip assets=imported:5
```

(5 bundles: the four mart datasets + the Sales, Executive, Customer and
Marketing dashboards.)

- `admin`: `create` on first boot, `reset_password` after a `.env` password
  rotation, `skip` otherwise;
- `*_connection`: keyed by display name; the URI is compared against the
  **decrypted** stored URI (Superset masks `sqlalchemy_uri` once the asset
  import has run). Connections always converge on the fixed UUIDs pinned in
  `infrastructure/scripts/superset_init.py` — exported bundles reference
  them, which is what keeps credentials out of Git;
- `assets`: every ZIP bundle under `superset/assets/` is imported with
  overwrite semantics (datasets, charts, dashboards upserted by UUID) — the
  repository is the source of truth for BI content.

Web UI: `http://127.0.0.1:8088` (loopback only). Log in with
`SUPERSET_ADMIN_USER` / `SUPERSET_ADMIN_PASSWORD` from `.env`. Dashboard
URLs are slug-based and stable across re-imports:
`/superset/dashboard/sales|executive|customer|marketing/`.

## Ad-hoc exploration (Superset → Trino → Iceberg)

Dashboards always query the ClickHouse marts. SQL Lab carries the second
connection — switch the database dropdown to **Trino iceberg** to explore
the full lakehouse (`gold.fact_orders`, Silver/Bronze internals, any grain
the marts do not expose). Local Trino has no auth (docker-network only,
loopback-published host port) — a local-only posture, unchanged by Phase 7.

Prefer ClickHouse for repeated dashboard-style aggregation; prefer the
Trino path when you need modeled history or non-mart grains. The path is
asserted by `tests/integration/test_superset_bootstrap.py::test_sqllab_trino_adhoc_path`
(REST shape: `POST /api/v1/sqllab/execute/` with `database_id`, `sql`,
`queryLimit`, `runAsync: false` + the CSRF session cookie).

## BI-as-code round-trip

Change dashboards/datasets in the UI (or via the REST API), then export,
sanitize and commit — `superset-init` re-imports on the next `make bi-up`.

1. Find the object id: `GET /api/v1/dashboard/` (or `/api/v1/dataset/`)
   with an admin Bearer token from `POST /api/v1/security/login`.
2. Export (ZIP, import/export v1 format):

   ```bash
   curl --noproxy 127.0.0.1 -H "Authorization: Bearer $TOKEN" \
     "http://127.0.0.1:8088/api/v1/dashboard/export/?q=!(<id>)" \
     -o superset/assets/<name>_dashboard.zip
   # datasets only:
   curl --noproxy 127.0.0.1 -H "Authorization: Bearer $TOKEN" \
     "http://127.0.0.1:8088/api/v1/dataset/export/?q=!(<id1>,<id2>,...)" \
     -o superset/assets/datasets.zip
   ```

3. Sanitize before committing (strips the secret-bearing `databases/`
   entries, canonicalizes `metadata.yaml` to `type: assets`, makes the
   archive deterministic):

   ```bash
   uv run python infrastructure/scripts/superset_bundle_sanitize.py
   ```

   Never skip this step: raw exports contain the ClickHouse URI **with the
   password**. `tests/unit/serving/superset/test_superset_assets.py` guards
   the committed tree against regressions.

4. Verify the loop: `make bi-down && make bi-up`, then check the summary
   line shows `assets=imported:N` and the dashboard renders. From a truly
   clean slate (fresh clone simulation): remove the metadata volume first —

   ```bash
   make bi-down
   docker volume rm omni-retail_superset-metadata-data   # name may carry a different compose project prefix
   make bi-up
   ```

   Everything (schema, admin, connections, dashboards) is re-created from
   the repository plus `.env`.

Import mechanics (why the sanitizer exists): the Superset assets importer
resolves every dataset's `database_uuid` against a database config inside
the bundle. Committed bundles strip that config; `superset-init` injects an
equivalent config built from `.env` into the in-memory contents at import
time, keyed by the fixed connection UUID. Credentials exist only in `.env`
and in memory.

## Secret rotation

- Reader password (`CLICKHOUSE_READER_PASSWORD`): edit `.env`, then
  `make bi-up` — the connection upsert detects the changed (decrypted) URI
  and updates it; dashboards keep working.
- Admin password (`SUPERSET_ADMIN_PASSWORD`): same — the bootstrap resets it
  on the next run.
- `SUPERSET_SECRET_KEY`: changing it invalidates session cookies and the
  encrypted connection passwords in the metadata DB. Rotate only together
  with a metadata-volume rebuild (connections are re-created from `.env`).

## Common failures

| Symptom | Cause / fix |
| ------- | ----------- |
| `make bi-up` hangs waiting for `superset` health | Metadata DB not healthy yet or `SUPERSET_SECRET_KEY` unset — check `docker compose logs superset-postgres superset`. The web service refuses to start with an unset secret (by design). |
| `assets=imported:0` never appears / `assets=skip` | `superset/assets/` contains no `*.zip` (clean clone before first export) — expected only while the tree is empty. |
| Import fails with `CommandInvalidError: Error importing assets` | Bundle `metadata.yaml` not canonical (`type: assets`) or schema mismatch after a Superset upgrade — re-export from the upgraded instance and re-sanitize (the bundle format is version-sensitive; ADR 0005). |
| Dashboards show connection errors after rotating the reader password | `.env` and metadata DB diverged — re-run `make bi-up`; the upsert aligns the URI (see Lifecycle). |
| REST mutations return `The CSRF token is missing` | Superset requires `X-CSRFToken` (+ `Referer`) on POST/PUT/DELETE even with a Bearer token — fetch `GET /api/v1/security/csrf_token/` first. |
| `GET /api/v1/chart/<id>/data/` returns "no query context saved" | v2 charts are client-rendered; use `POST /api/v1/chart/data` with a QueryContext (see `tests/integration/test_superset_bootstrap.py` for the shape). |

## Dashboard screenshots

Screenshots of every delivered dashboard belong in the main README gallery;
the capture procedure (URLs by slug, save-as names, login) lives in
[`docs/screenshots/README.md`](../screenshots/README.md). Capture from the web
UI and commit the JPEG files to `docs/screenshots/` using the stable names
documented there.
