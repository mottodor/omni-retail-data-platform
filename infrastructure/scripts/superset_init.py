"""Idempotent Superset bootstrap for the OmniRetail BI layer (ADR 0005).

Runs as the one-shot ``superset-init`` container (profile ``bi``) after the
Superset web service reports healthy:

1. ``superset db upgrade``   — schema migrations (safe to re-run);
2. ``superset init``         — default roles/permissions (idempotent);
3. admin user upsert         — create once, keep the password in sync with
                               ``.env`` (rotation support), never duplicate;
4. database connection upserts keyed by ``database_name``:
   "ClickHouse analytics" (superset_reader, docker-network hostname) and
   "Trino iceberg" (ad-hoc exploration path);
5. asset import from the mounted ``superset/assets/`` tree — skipped with a
   log line while the tree is empty (Phase 7 slice 1); slices 2-3 commit
   exported ZIP bundles there and they are re-imported on every bootstrap.

Only the standard library is imported at module level so the pure decision
functions are unit-testable without Superset installed; the Superset Python
API is imported inside the functions that need it.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

#: Display names double as upsert keys inside the Superset metadata DB.
CLICKHOUSE_CONNECTION_NAME = "ClickHouse analytics"
TRINO_CONNECTION_NAME = "Trino iceberg"

#: Committed BI assets (import/export v1 ZIP bundles) are imported from here.
DEFAULT_ASSETS_DIR = "/app/superset_home/repo/assets"


def build_clickhouse_uri(*, user: str, password: str, host: str, port: int, database: str) -> str:
    """Build the Superset SQLAlchemy URI for the ClickHouse serving layer.

    Engine ``clickhousedb`` with the default driver ``connect``
    (clickhouse-connect — the Superset-recommended connector, HTTP port).
    """
    return f"clickhousedb://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{database}"


def build_trino_uri(*, user: str, host: str, port: int, catalog: str) -> str:
    """Build the Superset SQLAlchemy URI for the Trino exploration path."""
    return f"trino://{quote_plus(user)}@{host}:{port}/{catalog}"


def decide_user_action(*, exists: bool, password_matches: bool) -> str:
    """Admin upsert decision: create once, follow .env on rotation, else skip."""
    if not exists:
        return "create"
    if not password_matches:
        return "reset_password"
    return "skip"


def decide_database_action(*, exists: bool, uri_matches: bool) -> str:
    """Connection upsert decision keyed by database_name (idempotent re-runs)."""
    if not exists:
        return "create"
    if not uri_matches:
        return "update"
    return "skip"


def find_asset_bundles(assets_dir: Path) -> list[Path]:
    """Return committed import/export v1 ZIP bundles, sorted for determinism."""
    if not assets_dir.is_dir():
        return []
    return sorted(path for path in assets_dir.glob("*.zip") if path.is_file())


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"required environment variable {name} is not set")
    return value


def _required_env_int(name: str) -> int:
    return int(_required_env(name))


def _run_cli(*args: str) -> None:
    """Run a ``superset`` CLI subcommand; non-zero exit fails the bootstrap."""
    subprocess.run(["superset", *args], check=True)


def _admin_email(username: str) -> str:
    return f"{username}@omni-retail.local"


def upsert_admin(appbuilder: Any, *, username: str, password: str) -> str:
    """Create the admin user or re-point its password at the .env value."""
    from werkzeug.security import check_password_hash

    sm = appbuilder.sm
    existing = sm.find_user(username=username)
    password_matches = bool(
        existing is not None
        and existing.password
        and check_password_hash(existing.password, password)
    )
    action = decide_user_action(exists=existing is not None, password_matches=password_matches)
    if action == "skip":
        return action
    if action == "create":
        admin_role = sm.find_role("Admin")
        if admin_role is None:
            raise RuntimeError("role 'Admin' not found — did `superset init` run?")
        sm.add_user(
            username=username,
            first_name="OmniRetail",
            last_name="Admin",
            email=_admin_email(username),
            role=admin_role,
            password=password,
        )
        return action
    # reset_password (FAB): werkzeug-hashes the value and commits — keeps
    # .env rotation working on re-runs.
    assert existing is not None
    sm.reset_password(existing.id, password)
    return action


def upsert_database(*, name: str, sqlalchemy_uri: str, session: Any) -> str:
    """Upsert one database connection keyed by its display name."""
    from superset.models.core import Database

    existing = session.query(Database).filter_by(database_name=name).one_or_none()
    action = decide_database_action(
        exists=existing is not None,
        uri_matches=existing is not None and existing.sqlalchemy_uri == sqlalchemy_uri,
    )
    if action == "create":
        session.add(
            Database(
                database_name=name,
                sqlalchemy_uri=sqlalchemy_uri,
                impersonate_user=False,
            )
        )
        session.commit()
    elif action == "update":
        assert existing is not None
        existing.sqlalchemy_uri = sqlalchemy_uri
        session.commit()
    return action


def import_assets(*, assets_dir: Path) -> str:
    """Import committed BI assets; an empty tree is a documented skip."""
    bundles = find_asset_bundles(assets_dir)
    if not bundles:
        return "skip"
    from superset.commands.importers.v1 import ImportV1Command

    for bundle in bundles:
        ImportV1Command(str(bundle), overwrite=True).run()
    return f"imported:{len(bundles)}"


def main() -> int:
    admin_user = _required_env("SUPERSET_ADMIN_USER")
    admin_password = _required_env("SUPERSET_ADMIN_PASSWORD")
    clickhouse_uri = build_clickhouse_uri(
        user=_required_env("CLICKHOUSE_READER_USER"),
        password=_required_env("CLICKHOUSE_READER_PASSWORD"),
        host=_required_env("CLICKHOUSE_HOST"),
        port=_required_env_int("CLICKHOUSE_PORT"),
        database=_required_env("CLICKHOUSE_DB"),
    )
    trino_uri = build_trino_uri(
        user=_required_env("TRINO_USER"),
        host=_required_env("TRINO_HOST"),
        port=_required_env_int("TRINO_PORT"),
        catalog=_required_env("TRINO_CATALOG"),
    )
    assets_dir = Path(os.environ.get("SUPERSET_ASSETS_DIR", DEFAULT_ASSETS_DIR))

    _run_cli("db", "upgrade")
    _run_cli("init")

    from superset import db
    from superset.app import create_app

    app = create_app()
    with app.app_context():
        admin_action = upsert_admin(app.appbuilder, username=admin_user, password=admin_password)
        clickhouse_action = upsert_database(
            name=CLICKHOUSE_CONNECTION_NAME, sqlalchemy_uri=clickhouse_uri, session=db.session
        )
        trino_action = upsert_database(
            name=TRINO_CONNECTION_NAME, sqlalchemy_uri=trino_uri, session=db.session
        )
        assets_action = import_assets(assets_dir=assets_dir)

    print(
        "superset_init: bootstrap complete "
        f"admin={admin_action} "
        f"clickhouse_connection={clickhouse_action} "
        f"trino_connection={trino_action} "
        f"assets={assets_action}"
    )
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # noqa: BLE001 - one-shot job: fail with full context
        print(f"superset_init: FAILED error={type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
