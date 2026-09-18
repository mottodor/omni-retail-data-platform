# Phase 5 Follow-ups (Infra) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the three follow-ups from the Phase 5 slice 1 report: Polaris DROP authorization/feature gap, operational purge of PostgreSQL extraction watermarks, and `pip check` conflicts inside the custom Airflow image.

**Architecture:** Three independent fixes. (1) `polaris_init.py` (one-shot compose job) additionally grants `CATALOG_MANAGE_CONTENT` to the auto-created `catalog_admin` role via the Polaris management API, and the `polaris` service enables `DROP_WITH_PURGE_ENABLED` — the exact combination Trino's own Polaris integration tests use, because Trino 483 always sends `dropTable(purge=true)` on REST catalogs. (2) The `postgres_snapshot` CLI gains a `purge-watermarks` subcommand backed by a pure `purge_watermarks()` function. (3) The uv lockfile is constrained to versions compatible with the frozen `apache/airflow:2.11.2-python3.12` base-image packages, plus a pinned `grpcio-status` overlay and a build-time `pip check` guard in the Airflow Dockerfile.

**Tech Stack:** Python 3.12 + uv (stdlib-only for polaris_init); Docker Compose; pytest.

**Spec:** `docs/superpowers/specs/2026-09-18-phase5-dbt-lakehouse-design.md` (§6 error policies, §7 dependency risk) plus the empirical root causes reproduced on the live stack (documented per task below).

## Root causes (verified live, 2026-09-18/19)

1. **Polaris DROP 403 / purge failure.** Trino 483's `TrinoRestCatalog.dropTable` always calls `purgeTable` (`dropTable(purge=true)`); there is no Trino-side switch (checked `plugin/trino-iceberg/.../TrinoRestCatalog.java` @ 483). Polaris 1.7.0 then requires `TABLE_WRITE_DATA` **on the table itself** for `DROP_TABLE_WITH_PURGE`, and the auto-created `catalog_admin` role only carries `CATALOG_MANAGE_ACCESS` + `CATALOG_MANAGE_METADATA` (queried via `GET /api/management/v1/catalogs/lakehouse/catalog-roles/catalog_admin/grants`). After granting `CATALOG_MANAGE_CONTENT`, authorization passes but the operation fails unless `polaris.features."DROP_WITH_PURGE_ENABLED"` is `true` (default `false`) — including for `DROP VIEW`. Trino's own `TestIcebergPolarisCatalogConnectorSmokeTest` sets exactly this env var. Management API quirk: **PUT** `/grants` adds a grant, **POST** revokes (verified against `integration-tests/.../ManagementApi.java` and live).
2. **Stale watermarks after source re-seed.** `make generate-oltp` can truncate + reload the OLTP source; `_watermarks/postgres/*.json` in the archive bucket survive and would make subsequent incremental extracts silently skip rows (new `updated_at` values can sort before the old keyset position).
3. **`pip check` conflicts in the Airflow image** (5, reproduced via `docker run --rm omni-retail/airflow:0.1.0 pip check`): `aiobotocore 3.2.1` wants `botocore<1.42.62`; `grpcio-status 1.71.2` wants `protobuf<6` (our dbt requires `protobuf>=6`); `opentelemetry-sdk 1.40.0` + `opentelemetry-semantic-conventions 0.61b0` want `opentelemetry-api==1.40.0`; `awswrangler 3.15.1` wants `pyarrow<23`.

## Global Constraints

- Python 3.12; uv only (`uv lock` / `uv sync`, never pip in the dev env); `uv.lock` committed.
- ruff rules `E,F,I,UP,B,SIM`, line length 100; `mypy --strict` over `omni_retail` (polaris_init.py is infrastructure-only and tested via its own pure-function test module, not mypy packages).
- `polaris_init.py` must stay stdlib-only (runs in plain `python:3.12-alpine`).
- No secrets committed; grants/feature flags are not secrets.
- Branch `feature/phase5-followups` off `main`; Conventional Commits (`fix(infra):`, `fix(ingestion):`, `chore(deps):`); separate PR from slice 2.
- Every live-stack change must be verified on the running core profile before the task is called done.
- Live-stack residue to clean up during Task 2: `iceberg.scratch.t_drop_probe` (table) and `iceberg.scratch.v_probe` (view) left over from the root-cause probing, plus the `scratch` schema itself.

---

### Task 1: polaris_init grants CATALOG_MANAGE_CONTENT to catalog_admin

**Files:**
- Modify: `infrastructure/scripts/polaris_init.py`
- Test: `tests/test_polaris_init.py`

**Interfaces:**
- Consumes: existing `obtain_token`, `_management_request`, `decide_action`.
- Produces:
  - `CATALOG_ADMIN_ROLE: str = "catalog_admin"`;
  - `REQUIRED_CATALOG_PRIVILEGES: tuple[str, ...] = ("CATALOG_MANAGE_CONTENT",)`;
  - `list_catalog_role_grants(*, base_url, token, realm, catalog_name, catalog_role_name=CATALOG_ADMIN_ROLE) -> list[dict[str, Any]]` (GET grants, returns the `grants` list, raises on unexpected status);
  - `decide_grant_action(grants: list[dict[str, Any]]) -> str` (pure: `"skip"` when every required catalog privilege is present, else `"grant"`);
  - `grant_catalog_privilege(*, base_url, token, realm, catalog_name, privilege, catalog_role_name=CATALOG_ADMIN_ROLE) -> None` (PUT grant, tolerates 200/201/409);
  - `main()` runs the grant-ensure step on **every** execution (create *and* skip paths) and prints `content_grant=<grant|skip>` in the summary line.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_polaris_init.py`)

```python
def test_decide_grant_action_skips_when_content_privilege_present() -> None:
    module = _load_module()
    grants = [
        {"privilege": "CATALOG_MANAGE_ACCESS", "type": "catalog"},
        {"privilege": "CATALOG_MANAGE_METADATA", "type": "catalog"},
        {"privilege": "CATALOG_MANAGE_CONTENT", "type": "catalog"},
    ]
    assert module.decide_grant_action(grants) == "skip"


def test_decide_grant_action_grants_when_missing() -> None:
    module = _load_module()
    grants = [
        {"privilege": "CATALOG_MANAGE_ACCESS", "type": "catalog"},
        {"privilege": "CATALOG_MANAGE_METADATA", "type": "catalog"},
    ]
    assert module.decide_grant_action(grants) == "grant"


def test_decide_grant_action_grants_on_empty_grants() -> None:
    module = _load_module()
    assert module.decide_grant_action([]) == "grant"


def test_required_privileges_cover_drop_path() -> None:
    """CATALOG_MANAGE_CONTENT is what covers TABLE_DROP/VIEW_DROP inheritance
    and the TABLE_WRITE_DATA needed for Trino's purge-drops."""
    module = _load_module()
    assert module.REQUIRED_CATALOG_PRIVILEGES == ("CATALOG_MANAGE_CONTENT",)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_polaris_init.py -v`
Expected: FAIL with `AttributeError: ... no attribute 'decide_grant_action'`.

- [ ] **Step 3: Implement**

In `infrastructure/scripts/polaris_init.py`:

Add constants after `REQUEST_TIMEOUT_SECONDS`:

```python
#: Role auto-created by Polaris for every catalog; the grant target.
CATALOG_ADMIN_ROLE = "catalog_admin"

#: Content privileges the Trino principal needs for full table/view lifecycle
#: (create, drop, purge-drop). The auto-created catalog_admin only carries
#: CATALOG_MANAGE_ACCESS/METADATA, which does not cover Trino's
#: dropTable(purge=true) — root cause of the slice-1 teardown workaround.
REQUIRED_CATALOG_PRIVILEGES = ("CATALOG_MANAGE_CONTENT",)
```

Add functions after `create_catalog`:

```python
def list_catalog_role_grants(
    *,
    base_url: str,
    token: str,
    realm: str,
    catalog_name: str,
    catalog_role_name: str = CATALOG_ADMIN_ROLE,
) -> list[dict[str, Any]]:
    """List grants held by a catalog role (management API)."""
    status, payload = _management_request(
        method="GET",
        url=(
            f"{base_url}/api/management/v1/catalogs/{catalog_name}"
            f"/catalog-roles/{catalog_role_name}/grants"
        ),
        token=token,
        realm=realm,
    )
    if status != 200 or payload is None:
        raise RuntimeError(
            f"failed to list grants of {catalog_role_name!r} in {catalog_name}: "
            f"status={status} body={payload}"
        )
    grants = payload.get("grants")
    if not isinstance(grants, list):
        raise RuntimeError(f"unexpected grants payload for {catalog_role_name!r}: {payload}")
    return grants


def decide_grant_action(grants: list[dict[str, Any]]) -> str:
    """Return the grant step action: grant missing privileges or skip."""
    present = {
        (grant.get("type"), grant.get("privilege")) for grant in grants if isinstance(grant, dict)
    }
    missing = [
        privilege
        for privilege in REQUIRED_CATALOG_PRIVILEGES
        if ("catalog", privilege) not in present
    ]
    return "skip" if not missing else "grant"


def grant_catalog_privilege(
    *,
    base_url: str,
    token: str,
    realm: str,
    catalog_name: str,
    privilege: str,
    catalog_role_name: str = CATALOG_ADMIN_ROLE,
) -> None:
    """Add one catalog-level privilege to a catalog role (PUT adds, POST revokes)."""
    status, response = _management_request(
        method="PUT",
        url=(
            f"{base_url}/api/management/v1/catalogs/{catalog_name}"
            f"/catalog-roles/{catalog_role_name}/grants"
        ),
        token=token,
        realm=realm,
        body={"type": "catalog", "privilege": privilege},
    )
    if status not in (200, 201, 409):
        raise RuntimeError(
            f"failed to grant {privilege} to {catalog_role_name!r} in {catalog_name}: "
            f"status={status} body={response}"
        )
```

Update `main()`: after the create/skip decision block, always run:

```python
    grants = list_catalog_role_grants(
        base_url=base_url, token=token, realm=realm, catalog_name=catalog_name
    )
    grant_action = decide_grant_action(grants)
    if grant_action == "grant":
        for privilege in REQUIRED_CATALOG_PRIVILEGES:
            grant_catalog_privilege(
                base_url=base_url,
                token=token,
                realm=realm,
                catalog_name=catalog_name,
                privilege=privilege,
            )
```

and extend the final print:

```python
    print(
        f"polaris_init: catalog={catalog_name} base_location={base_location} "
        f"storage_endpoint={endpoint} action={action} content_grant={grant_action}"
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_polaris_init.py -v`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
git add infrastructure/scripts/polaris_init.py tests/test_polaris_init.py
git commit -m "fix(infra): grant CATALOG_MANAGE_CONTENT to catalog_admin in polaris-init"
```

---

### Task 2: enable purge-drops and verify DROP end-to-end

**Files:**
- Modify: `docker-compose.yml` (polaris service environment)
- Modify: `infrastructure/scripts/smoke_core.sh`

**Interfaces:**
- Produces: `polaris` service env `polaris.features."DROP_WITH_PURGE_ENABLED": "true"`; smoke-core asserts create+drop works.

- [ ] **Step 1: Add the feature flag to the polaris service**

In `docker-compose.yml`, in the `polaris` service `environment:` block, right after the existing `polaris.features."ALLOW_INSECURE_STORAGE_TYPES"` line, add with a comment:

```yaml
      # Trino 483 always issues dropTable(purge=true) on REST catalogs, and
      # Polaris purge-drops (tables AND views) require this flag; without it
      # every DROP from Trino fails authorization/feature checks.
      polaris.features."DROP_WITH_PURGE_ENABLED": "true"
```

- [ ] **Step 2: Extend smoke-core with a drop probe**

In `infrastructure/scripts/smoke_core.sh`, after the row-count check and before the final `echo "smoke-core: PASS ..."` line, add:

```bash
# Regression guard (Phase 5 follow-up): Trino must be able to DROP objects
# (purge-drop path + views) through Polaris.
trino_exec "DROP TABLE IF EXISTS iceberg.demo.drop_probe" >/dev/null
trino_exec "CREATE TABLE iceberg.demo.drop_probe (probe_id int)" >/dev/null
trino_exec "DROP TABLE iceberg.demo.drop_probe" >/dev/null
```

- [ ] **Step 3: Validate compose config**

```bash
docker compose config --quiet
```
Expected: no output (valid; uses the real local `.env` — never clobber it with `.env.example`).

- [ ] **Step 4: Apply on the live stack and verify DROP TABLE / VIEW / SCHEMA**

```bash
docker compose up -d polaris          # recreate with the new env
docker compose up polaris-init        # re-run initializer: idempotent + grant step
```

Then verify the previously-stuck objects from the root-cause probing are now droppable, cleaning up the residue:

```bash
docker compose exec -T trino trino --output-format=CSV --execute "DROP TABLE iceberg.scratch.t_drop_probe"
docker compose exec -T trino trino --output-format=CSV --execute "DROP VIEW iceberg.scratch.v_probe"
docker compose exec -T trino trino --output-format=CSV --execute "DROP SCHEMA iceberg.scratch"
make smoke-core
```

Expected: all three DROPs succeed; smoke-core prints `smoke-core: PASS ...` (including the new drop probe).

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml infrastructure/scripts/smoke_core.sh
git commit -m "fix(infra): enable Polaris purge-drops for Trino and guard them in smoke-core"
```

---

### Task 3: bronze integration teardown uses real drops where safe

**Files:**
- Modify: `tests/integration/test_bronze_load.py` (fixture `clean_bronze`)

**Interfaces:**
- Consumes: working `DROP VIEW` from Task 2.
- Produces: teardown drops the test-built `silver.stg_orders` view; bronze partitions still cleaned by `DELETE` (other partitions of a shared local stack must survive).

- [ ] **Step 1: Update the fixture**

In `tests/integration/test_bronze_load.py`, replace the `clean_bronze` fixture body/comment with:

```python
@pytest.fixture()
def clean_bronze(live_storage: BotoObjectStorage) -> Generator[None, None, None]:
    purge_raw(live_storage)
    yield
    # Teardown: remove the loaded DATA by partition (a shared local stack may
    # hold other partitions of the same bronze tables) and drop the dbt-built
    # staging view — Trino purge-drops work since the Phase 5 infra follow-up
    # (CATALOG_MANAGE_CONTENT grant + DROP_WITH_PURGE_ENABLED).
    partition = f"\"_batch_date\" = DATE '{LOGICAL_DATE:%Y-%m-%d}'"
    for table in ("orders", "fx_rates"):
        if bronze_table_exists(table):
            trino_scalar(f"delete from iceberg.bronze.{table} where {partition}")
    trino_scalar("drop view if exists iceberg.silver.stg_orders")
    purge_raw(live_storage)
```

- [ ] **Step 2: Run the integration suite (live core stack)**

```bash
make integration
```
Expected: all integration tests PASS (postgres snapshot, api, files, bronze).

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_bronze_load.py
git commit -m "test(integration): drop stg view in bronze teardown now that Polaris allows it"
```

---

### Task 4: purge-watermarks CLI for PostgreSQL extraction

**Files:**
- Modify: `src/omni_retail/ingestion/postgres_snapshot/watermark.py`
- Modify: `src/omni_retail/ingestion/postgres_snapshot/cli.py`
- Test: `tests/unit/ingestion/postgres_snapshot/test_watermark.py`
- Test: `tests/unit/ingestion/postgres_snapshot/test_snapshot_cli.py`
- Modify: `tests/integration/test_postgres_snapshot.py` (fixture reuse)
- Modify: `README.md` (one sentence in the snapshot paragraph)

**Interfaces:**
- Consumes: `ObjectStorage` protocol, `BUCKET_ARCHIVE`, `postgres_watermark_key`, `TABLES` from `tables.py`.
- Produces: `purge_watermarks(storage: ObjectStorage, tables: Sequence[str] | None = None) -> tuple[str, ...]`; CLI subcommand `purge-watermarks [--table <name>]` (exit 0, logs purged keys; unknown table names are argparse-validated).

- [ ] **Step 1: Write the failing tests** (append to `tests/unit/ingestion/postgres_snapshot/test_watermark.py`; check the file's existing imports and reuse its `FakeStorage` import path — it already imports from `fakes.storage` and `omni_retail.ingestion.postgres_snapshot.watermark`)

Add the fixture near the top of the file (after imports/constants):

```python
@pytest.fixture()
def purger_storage() -> FakeStorage:
    return FakeStorage()
```

Then append the tests (the file already imports `pytest`, `FakeStorage`, `postgres_watermark_key` and `TABLES`; extend the `paths` import with `BUCKET_ARCHIVE` and the `watermark` import with `purge_watermarks`):

```python
def test_purge_watermarks_all_tables(purger_storage: FakeStorage) -> None:
    for name in ("orders", "customers"):
        purger_storage.put_object(BUCKET_ARCHIVE, postgres_watermark_key(name), b"{}")

    purged = purge_watermarks(purger_storage)

    assert sorted(purged) == [
        "_watermarks/postgres/customers.json",
        "_watermarks/postgres/orders.json",
    ]
    assert purger_storage.list_object_keys(BUCKET_ARCHIVE, "_watermarks/postgres/") == ()


def test_purge_watermarks_single_table(purger_storage: FakeStorage) -> None:
    for name in ("orders", "customers"):
        purger_storage.put_object(BUCKET_ARCHIVE, postgres_watermark_key(name), b"{}")

    purged = purge_watermarks(purger_storage, tables=["orders"])

    assert purged == ("_watermarks/postgres/orders.json",)
    assert purger_storage.object_exists(BUCKET_ARCHIVE, postgres_watermark_key("customers"))


def test_purge_watermarks_is_idempotent(purger_storage: FakeStorage) -> None:
    purger_storage.put_object(BUCKET_ARCHIVE, postgres_watermark_key("orders"), b"{}")

    assert purge_watermarks(purger_storage, tables=["orders"]) == (
        "_watermarks/postgres/orders.json",
    )
    assert purge_watermarks(purger_storage, tables=["orders"]) == ()


def test_purge_watermarks_rejects_unknown_table(purger_storage: FakeStorage) -> None:
    with pytest.raises(ValueError, match="unknown snapshot table"):
        purge_watermarks(purger_storage, tables=["nope"])
```

Adjust the imports of the test file to also bring in `TABLES` from `omni_retail.ingestion.postgres_snapshot.tables` and `purge_watermarks`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/ingestion/postgres_snapshot/test_watermark.py -v`
Expected: FAIL with `ImportError: cannot import name 'purge_watermarks'`.

- [ ] **Step 3: Implement `purge_watermarks`** (append to `watermark.py`; extend the module's imports with `from collections.abc import Sequence` and `from omni_retail.ingestion.postgres_snapshot.tables import TABLES`)

```python
def purge_watermarks(
    storage: ObjectStorage, tables: Sequence[str] | None = None
) -> tuple[str, ...]:
    """Delete durable watermarks (operator action, e.g. after re-seeding OLTP).

    Missing watermarks are skipped, so the operation is idempotent. With
    ``tables=None`` every snapshot table's watermark is addressed. Returns the
    purged object keys in registry order.
    """
    names = tuple(TABLES) if tables is None else tuple(tables)
    unknown = [name for name in names if name not in TABLES]
    if unknown:
        known = ", ".join(sorted(TABLES))
        raise ValueError(f"unknown snapshot table(s) {unknown} (known: {known})")

    purged: list[str] = []
    for name in names:
        key = postgres_watermark_key(name)
        if storage.object_exists(BUCKET_ARCHIVE, key):
            storage.delete_object(BUCKET_ARCHIVE, key)
            purged.append(key)
    return tuple(purged)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/ingestion/postgres_snapshot/test_watermark.py -v`
Expected: PASS.

- [ ] **Step 5: Add the CLI subcommand** (`tests` first, then `cli.py`)

Append to `tests/unit/ingestion/postgres_snapshot/test_snapshot_cli.py`:

```python
def test_purge_watermarks_defaults_to_all_tables() -> None:
    args = build_parser().parse_args(["purge-watermarks"])
    assert args.command == "purge-watermarks"
    assert args.table is None


def test_purge_watermarks_parses_known_table() -> None:
    args = build_parser().parse_args(["purge-watermarks", "--table", "orders"])
    assert args.table == "orders"


def test_purge_watermarks_rejects_unknown_table() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["purge-watermarks", "--table", "nope"])
```

Run: `uv run pytest tests/unit/ingestion/postgres_snapshot/test_snapshot_cli.py -v` — expect the two new parse tests to FAIL (`invalid choice` for the subcommand).

Implement in `cli.py`:

In `build_parser()`, after the `run` subparser block:

```python
    purge_parser = subparsers.add_parser(
        "purge-watermarks",
        help=(
            "delete durable extraction watermarks (all tables, or --table); "
            "use after re-seeding the OLTP source so incremental extracts "
            "do not skip freshly generated rows"
        ),
    )
    purge_parser.add_argument(
        "--table",
        choices=sorted(TABLES),
        default=None,
        help="restrict the purge to one snapshot table (default: all tables)",
    )
```

Extend the `cli.py` imports: `from omni_retail.ingestion.postgres_snapshot.tables import TABLES, table_by_name` (replacing the current `table_by_name` import) and `from omni_retail.ingestion.postgres_snapshot.watermark import load_watermark, purge_watermarks`.

Restructure `main()` so the purge branch runs before any PostgreSQL connection (it only needs storage) and storage construction stays inside the existing `try` block so `StorageError` is handled for both commands:

```python
def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = build_parser().parse_args(argv)
    try:
        storage = BotoObjectStorage(StorageConfig.from_env())
        if args.command == "purge-watermarks":
            tables = [args.table] if args.table else None
            purged = purge_watermarks(storage, tables=tables)
            logger.info(
                "purge-watermarks result: purged=%d keys=%s",
                len(purged),
                list(purged),
            )
            return 0

        spec = table_by_name(args.table)
        config = PostgresSourceConfig.from_env()
        with psycopg.connect(config.conninfo()) as conn:
            watermark = None if args.full_refresh else load_watermark(storage, spec)
            manifest = snapshot_table(
                storage,
                cast(SnapshotConnection, conn),
                spec,
                logical_date=args.date,
                watermark=watermark,
            )
        log = context_logger(__name__, source=spec.source_name, logical_date=args.date.isoformat())
        log.info(
            "run result: batch_id=%s status=%s row_count=%d object_key=%s",
            manifest.batch_id,
            manifest.status,
            manifest.row_count,
            manifest.object_key,
        )
        return 0
    except (ValueError, StorageError, psycopg.Error) as error:
        logger.error("%s failed: %s", args.command, error)
        return 1
```

(The only changes to the run path: `storage` construction moves to the top of `try`, and the error log names the command.)

Run: `uv run pytest tests/unit/ingestion/postgres_snapshot/test_snapshot_cli.py -v` — expect PASS.

- [ ] **Step 6: Reuse in the integration fixture**

In `tests/integration/test_postgres_snapshot.py`, replace the third tuple entry handling in `purge_customers_namespace` so the watermark deletion goes through the new function (keeps prod/teardown semantics identical):

```python
def purge_customers_namespace(storage: BotoObjectStorage) -> None:
    """Delete only the customers snapshot namespace (other tables untouched)."""
    for bucket, prefix in (
        (BUCKET_ARCHIVE, "postgres/customers/"),
        (BUCKET_ARCHIVE, "_manifests/postgres-customers/"),
    ):
        for key in storage.list_object_keys(bucket, prefix):
            storage.delete_object(bucket, key)
    purge_watermarks(storage, tables=["customers"])
```

with `purge_watermarks` added to the file's imports from `omni_retail.ingestion.postgres_snapshot.watermark`.

- [ ] **Step 7: Document in README**

In `README.md`, in the PostgreSQL-snapshot paragraph (the one describing watermarks), append one sentence after "…same window.":

```
After re-seeding the source (`make generate-oltp` with a truncate), purge the
stale watermarks first: `uv run python -m omni_retail.ingestion.postgres_snapshot purge-watermarks`
(all tables) or `… purge-watermarks --table orders` (one table).
```

- [ ] **Step 8: Run the full unit suite + lint + mypy**

```bash
uv run pytest && uv run ruff check . && uv run ruff format --check . && uv run mypy src tests
```
Expected: all PASS.

- [ ] **Step 9: Commit**

```bash
git add src/omni_retail/ingestion/postgres_snapshot tests/unit/ingestion/postgres_snapshot tests/integration/test_postgres_snapshot.py README.md
git commit -m "fix(ingestion): add purge-watermarks CLI for stale extraction watermarks"
```

---

### Task 5: align lockfile + Airflow image to a clean pip check

**Files:**
- Modify: `pyproject.toml`
- Modify: `uv.lock` (regenerated)
- Modify: `infrastructure/airflow/Dockerfile`

**Interfaces:**
- Produces: `[tool.uv] constraint-dependencies` aligned with the frozen Airflow base image; pinned `grpcio-status==1.78.0` overlay; build-time `pip check` guard.

- [ ] **Step 1: Add constraints to pyproject.toml**

Append at the end of `pyproject.toml` (new section; verify no existing `[tool.uv]` — if one exists, merge):

```toml
[tool.uv]
# Constraints keep shared libraries compatible with the packages frozen inside
# apache/airflow:2.11.2-python3.12, so the overlay installed into the custom
# Airflow image (uv export --no-dev) stays `pip check` clean:
#   aiobotocore 3.2.1            -> botocore<1.42.62
#   awswrangler 3.15.1           -> pyarrow<23
#   opentelemetry-sdk 1.40.0     -> opentelemetry-api==1.40.0
constraint-dependencies = [
    "botocore<1.42.62",
    "opentelemetry-api==1.40.0",
    "pyarrow<23",
]
```

- [ ] **Step 2: Re-lock and sync**

```bash
uv lock && uv sync
```

Verify the resolution moved: `uv pip list | grep -E "boto3|botocore|pyarrow|opentelemetry-api"` — expect boto3/botocore 1.42.61.x, pyarrow 22.x, opentelemetry-api 1.40.0. Then:

```bash
uv pip check && uv run pytest && uv run ruff check . && uv run ruff format --check . \
  && uv run mypy src tests && uv run dbt parse --project-dir dbt --profiles-dir dbt
```
Expected: everything PASS (dev environment unaffected functionally).

- [ ] **Step 3: Update the Airflow Dockerfile**

In `infrastructure/airflow/Dockerfile`, after the pinned-pytest `RUN` and before the `COPY --chown=airflow:0 src/omni_retail …` line, add:

```dockerfile
# dbt requires protobuf>=6, but the base image ships grpcio-status 1.71.2
# (protobuf<6). Overlay the newest grpcio-status compatible with both the
# image (grpcio 1.78.0) and our protobuf 6.x so `pip check` stays clean.
RUN uv pip install --target "${AIRFLOW_SITE_PACKAGES}" "grpcio-status==1.78.0"
```

And after the `RUN chown -R airflow:0 "${AIRFLOW_SITE_PACKAGES}"` line, before `ENV PYTHONPATH=/opt/airflow`, add:

```dockerfile
# Guard: the overlay must stay consistent with the base image's frozen set.
USER airflow
RUN python -m pip check
```

(`USER airflow` appears again at the end of the file; the duplicate is harmless and keeps the guard close to the installs.)

- [ ] **Step 4: Rebuild the image and verify inside it**

```bash
make airflow-build
docker run --rm --entrypoint /bin/bash omni-retail/airflow:0.1.0 -c "python -m pip check"
```
Expected: build succeeds (the in-build `pip check` guard passes) and the manual run prints `No broken requirements found.`

- [ ] **Step 5: Run the DAG test harness**

```bash
AIRFLOW_SKIP_BUILD=1 bash infrastructure/scripts/airflow_tests.sh
```
Expected: pytest + `airflow dags list-import-errors` clean.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml uv.lock infrastructure/airflow/Dockerfile
git commit -m "chore(deps): align lockfile with Airflow image and guard pip check in build"
```

---

### Task 6: final validation, branch, PR

- [ ] **Step 1: Full local validation**

```bash
make lint && make test
uv run dbt parse --project-dir dbt --profiles-dir dbt
make smoke-core
make integration
AIRFLOW_SKIP_BUILD=1 bash infrastructure/scripts/airflow_tests.sh
```
Expected: all PASS.

- [ ] **Step 2: Push and open PR**

```bash
git checkout -b feature/phase5-followups   # if not already on it
git push -u origin feature/phase5-followups
gh pr create --title "fix(infra): Polaris purge-drops, watermark purge, Airflow image pip check" \
  --body "Closes the three Phase 5 slice-1 follow-ups. See docs/superpowers/plans/2026-09-19-phase5-followups-infra.md for root causes and verification." || true
```

If `gh` is unavailable, report the branch name and let the user open the PR.

- [ ] **Step 3: Report**

Produce the AGENTS §49 completion report (implemented / changed files / validation actually executed / manual verification / known limitations / follow-up).
