"""Live bootstrap checks for the Superset BI layer (Phase 7, ADR 0005).

Requires the full stacks up: ``make up && make bi-up`` (which builds the
custom image and runs the one-shot ``superset-init``), then run via
``make integration``. Verified here:

- the web service answers ``/health``;
- the bootstrap created the admin user (REST login succeeds);
- both database connections exist with docker-network hostnames and the
  least-privilege accounts (guide §26.4);
- the ClickHouse reader account can actually query the serving marts;
- the committed BI bundles are imported: the four mart datasets and the
  Sales, Executive, Customer and Marketing dashboards exist (Phase 7);
- the Trino ad-hoc path works: a SQL Lab query executes against the
  ``iceberg`` catalog through the same REST endpoint the SQL Lab UI uses;
- canary queries through the Superset data API reconcile with direct
  ClickHouse queries over the same marts (lightweight echo of the Phase 6
  reconciliation idea).
"""

import math
import os
from typing import Any

import clickhouse_connect
import httpx
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError

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

    denied = pytest.raises(DatabaseError)
    with denied:
        client.command("CREATE TABLE analytics.reader_must_not_create (id UInt8) ENGINE = Memory")


EXPECTED_DATASETS = {
    "mart_daily_sales": {"revenue", "margin", "orders", "items_sold", "aov"},
    "mart_customer_ltv": {"customers", "gmv"},
    "mart_marketing_roi": {"spend", "impressions", "clicks"},
    "mart_delivery_performance": {"deliveries", "delivered", "delayed"},
}

SALES_DASHBOARD_TITLE = "Sales"

#: Dashboard title -> expected chart count (guards the import completeness
#: of every committed bundle, not just Sales).
EXPECTED_DASHBOARDS = {
    "Sales": 10,
    "Executive": 11,
    "Customer": 11,
    "Marketing": 9,
}

#: Set by the fixtures below; the SQL Lab endpoint needs the CSRF session
#: cookie alongside the bearer token (same as the UI).


class SupersetSession:
    """Authorized REST session with a CSRF token and its session cookie."""

    def __init__(self, token: str) -> None:
        self.client = httpx.Client(
            base_url=SUPERSET_BASE_URL, timeout=REQUEST_TIMEOUT_SECONDS, trust_env=False
        )
        self.client.headers["Authorization"] = f"Bearer {token}"
        csrf = self.client.get("/api/v1/security/csrf_token/").json()["result"]
        self.client.headers.update({"X-CSRFToken": csrf, "Referer": SUPERSET_BASE_URL})

    def get(self, path: str, **kwargs: Any) -> httpx.Response:
        return self.client.get(path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> httpx.Response:
        return self.client.post(path, **kwargs)


@pytest.fixture()
def superset_session(admin_token: str) -> SupersetSession:
    return SupersetSession(admin_token)


def test_bootstrap_imported_bi_assets(admin_token: str) -> None:
    """The committed bundles re-create datasets + the Sales dashboard."""
    datasets = httpx.get(
        f"{SUPERSET_BASE_URL}/api/v1/dataset/",
        params={"limit": 100},
        headers={"Authorization": f"Bearer {admin_token}"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    assert datasets.status_code == 200, datasets.text
    by_table = {item["table_name"]: item["id"] for item in datasets.json()["result"]}
    for table in EXPECTED_DATASETS:
        assert table in by_table, f"dataset {table} missing; got {sorted(by_table)}"

    detail = httpx.get(
        f"{SUPERSET_BASE_URL}/api/v1/dataset/{by_table['mart_daily_sales']}",
        headers={"Authorization": f"Bearer {admin_token}"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    assert detail.status_code == 200, detail.text
    metric_names = {m["metric_name"] for m in detail.json()["result"]["metrics"]}
    assert metric_names == EXPECTED_DATASETS["mart_daily_sales"], metric_names

    dashboards = httpx.get(
        f"{SUPERSET_BASE_URL}/api/v1/dashboard/",
        params={"limit": 100},
        headers={"Authorization": f"Bearer {admin_token}"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    assert dashboards.status_code == 200, dashboards.text
    for item in dashboards.json()["result"]:
        title = item["dashboard_title"]
        expected = EXPECTED_DASHBOARDS.get(title)
        if expected is None:
            continue
        detail = httpx.get(
            f"{SUPERSET_BASE_URL}/api/v1/dashboard/{item['id']}",
            headers={"Authorization": f"Bearer {admin_token}"},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        assert detail.status_code == 200, detail.text
        charts = detail.json()["result"]["charts"]
        assert len(charts) == expected, (
            f"{title} dashboard must carry {expected} charts, got {len(charts)}"
        )
    missing = set(EXPECTED_DASHBOARDS) - {
        item["dashboard_title"] for item in dashboards.json()["result"]
    }
    assert not missing, f"dashboards missing from the bootstrap import: {sorted(missing)}"


def test_chart_canary_reconciles_with_clickhouse(superset_session: SupersetSession) -> None:
    """Totals through Superset's data API match direct reader queries."""
    datasets = superset_session.get("/api/v1/dataset/", params={"limit": 100}).json()["result"]
    dataset_ids = {item["table_name"]: item["id"] for item in datasets}

    def run_query(dataset: str, metrics: list[str]) -> dict[str, Any]:
        response = superset_session.post(
            "/api/v1/chart/data",
            json={
                "datasource": {"id": dataset_ids[dataset], "type": "table"},
                "queries": [{"metrics": metrics, "groupby": []}],
                "result_format": "json",
                "result_type": "full",
            },
        )
        assert response.status_code == 200, response.text
        payload = response.json()["result"][0]
        assert not payload.get("error"), payload["error"]
        data = payload["data"]
        assert data, "canary query returned no rows"
        row: dict[str, Any] = data[0]
        return row

    sales_row = run_query("mart_daily_sales", ["revenue", "orders", "aov"])
    ltv_row = run_query("mart_customer_ltv", ["gmv", "customers"])

    reader = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "127.0.0.1"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_READER_USER", "superset_reader"),
        password=os.environ.get("CLICKHOUSE_READER_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DB", "analytics"),
    )
    direct_sales = reader.query(
        "SELECT sum(revenue_eur), sum(orders_count), "
        "sum(revenue_eur) / sum(orders_count) FROM mart_daily_sales"
    ).result_rows[0]
    direct_ltv = reader.query("SELECT sum(gmv_eur), count() FROM mart_customer_ltv").result_rows[0]

    assert math.isclose(sales_row["revenue"], float(direct_sales[0]), rel_tol=1e-9)
    assert sales_row["orders"] == int(direct_sales[1])
    assert math.isclose(sales_row["aov"], float(direct_sales[2]), rel_tol=1e-9)
    assert math.isclose(ltv_row["gmv"], float(direct_ltv[0]), rel_tol=1e-9)
    assert ltv_row["customers"] == int(direct_ltv[1])


def test_sqllab_trino_adhoc_path(superset_session: SupersetSession) -> None:
    """The exploration path works: SQL Lab executes against the Iceberg catalog.

    Uses the same REST endpoint (``/api/v1/sqllab/execute/``) the SQL Lab UI
    posts to, with the session cookie the CSRF check requires — see the
    runbook's exploration section for when to prefer this path.
    """
    databases = superset_session.get("/api/v1/database/", params={"limit": 100}).json()["result"]
    trino_id = next(item["id"] for item in databases if item["database_name"] == "Trino iceberg")

    response = superset_session.post(
        "/api/v1/sqllab/execute/",
        json={
            "database_id": trino_id,
            "sql": "SELECT count(*) AS gold_rows FROM gold.fact_orders",
            "queryLimit": 100,
            "runAsync": False,
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["status"] == "success", payload
    rows = payload["data"]
    assert rows and rows[0]["gold_rows"] > 0, rows
