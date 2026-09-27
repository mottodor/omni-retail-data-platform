"""Live bootstrap checks for the Superset BI layer (Phase 7, ADR 0005).

Requires the full stacks up: ``make up && make bi-up`` (which builds the
custom image and runs the one-shot ``superset-init``), then run via
``make integration``. Verified here:

- the web service answers ``/health``;
- the bootstrap created the admin user (REST login succeeds);
- both database connections exist with docker-network hostnames and the
  least-privilege accounts (guide §26.4);
- the ClickHouse reader account can actually query the serving marts.
"""

import os

import clickhouse_connect
import httpx
import pytest

SUPERSET_PORT = os.environ.get("SUPERSET_PORT", "8088")
SUPERSET_BASE_URL = f"http://127.0.0.1:{SUPERSET_PORT}"
REQUEST_TIMEOUT_SECONDS = 15.0


@pytest.fixture()
def admin_token() -> str:
    """Log in as the admin bootstrapped from .env; failure means bad bootstrap."""
    response = httpx.post(
        f"{SUPERSET_BASE_URL}/api/v1/security/login",
        json={
            "username": os.environ.get("SUPERSET_ADMIN_USER", "admin"),
            "password": os.environ.get("SUPERSET_ADMIN_PASSWORD", ""),
            "provider": "db",
            "refresh": True,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    assert response.status_code == 200, (
        f"admin login failed: {response.status_code} {response.text}"
    )
    token = response.json().get("access_token")
    assert isinstance(token, str) and token, "login response carried no access_token"
    return token


def test_health_endpoint_reports_ok() -> None:
    response = httpx.get(f"{SUPERSET_BASE_URL}/health", timeout=REQUEST_TIMEOUT_SECONDS)
    assert response.status_code == 200
    assert response.text.startswith("OK")


def _fetch_databases(token: str) -> dict[str, str]:
    """Return {database_name: sqlalchemy_uri} from the Superset metadata API.

    The list/detail endpoints mask the URI in Superset 5; the dedicated
    ``/<pk>/connection`` endpoint exposes it to admins.
    """
    response = httpx.get(
        f"{SUPERSET_BASE_URL}/api/v1/database/",
        params={"limit": 100},
        headers={"Authorization": f"Bearer {token}"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    assert response.status_code == 200, response.text
    result: dict[str, str] = {}
    for item in response.json()["result"]:
        conn = httpx.get(
            f"{SUPERSET_BASE_URL}/api/v1/database/{item['id']}/connection",
            headers={"Authorization": f"Bearer {token}"},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        assert conn.status_code == 200, conn.text
        result[item["database_name"]] = conn.json()["result"]["sqlalchemy_uri"]
    return result


def test_bootstrap_created_both_connections(admin_token: str) -> None:
    databases = _fetch_databases(admin_token)

    clickhouse_uri = databases.get("ClickHouse analytics")
    assert clickhouse_uri, f"ClickHouse connection missing; got: {sorted(databases)}"
    reader_user = os.environ.get("CLICKHOUSE_READER_USER", "superset_reader")
    assert clickhouse_uri.startswith(f"clickhousedb://{reader_user}:"), (
        f"connection must use the {reader_user} account: {clickhouse_uri}"
    )
    assert "@clickhouse:8123/analytics" in clickhouse_uri, (
        f"connection must target the docker-network hostname: {clickhouse_uri}"
    )

    trino_uri = databases.get("Trino iceberg")
    assert trino_uri, f"Trino connection missing; got: {sorted(databases)}"
    assert trino_uri == "trino://omni_superset@trino:8080/iceberg", trino_uri


def test_reader_account_can_query_serving_marts() -> None:
    """The BI account is SELECT-only but sufficient for the published marts."""
    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "127.0.0.1"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_READER_USER", "superset_reader"),
        password=os.environ.get("CLICKHOUSE_READER_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DB", "analytics"),
    )
    result = client.query("SELECT count() FROM mart_daily_sales")
    row_count = int(result.result_rows[0][0])
    assert row_count >= 0, "reader query against the serving mart failed"

    denied = pytest.raises(clickhouse_connect.driver.exceptions.DatabaseError)
    with denied:
        client.command("CREATE TABLE analytics.reader_must_not_create (id UInt8) ENGINE = Memory")
