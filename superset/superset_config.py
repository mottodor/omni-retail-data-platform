"""Superset configuration for the local OmniRetail BI layer.

Loaded via ``SUPERSET_CONFIG_PATH`` (docker-compose.yml, ADR 0005). Every
value not set here keeps the upstream default from ``superset/config.py``;
only deployment-specific overrides live in this file.

The file is mounted read-only into the containers; secrets come from the
environment (AGENTS.md §10) and never from this repository.
"""

from __future__ import annotations

import os
from urllib.parse import quote_plus


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"required environment variable {name} is not set — "
            "Superset refuses to start with an unset secret/metadata config"
        )
    return value


# Superset signs session cookies and database-connection secrets with this
# key: fail loudly instead of silently falling back to a public default.
SECRET_KEY = _required_env("SUPERSET_SECRET_KEY")

# Dedicated metadata PostgreSQL (mirrors the Airflow ADR 0003 pattern):
# docker-network hostname, credentials from the environment, password
# URL-quoted so special characters cannot break the URI.
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{quote_plus(_required_env('SUPERSET_POSTGRES_USER'))}:"
    f"{quote_plus(_required_env('SUPERSET_POSTGRES_PASSWORD'))}"
    f"@superset-postgres:5432/{quote_plus(_required_env('SUPERSET_POSTGRES_DB'))}"
)

# Local loopback-only deployment (docs/adr/0005): no HTTPS termination in
# front, so Flask-Talisman stays off. Documented, not accidental.
TALISMAN_ENABLED = False

# MapBox maps are not used by any dashboard and would need an external API
# key — keep the offline-friendly default.
MAPBOX_API_KEY = ""

# Example data is never loaded (the bootstrap never runs `load-examples`);
# keep Jinja template processing off as the conservative default.
FEATURE_FLAGS: dict[str, bool] = {
    "ENABLE_TEMPLATE_PROCESSING": False,
}
