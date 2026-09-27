"""Guards for docker-compose configuration quality.

Enforces AGENTS.md infrastructure rules:
- pinned image versions (no latest/stable/edge);
- every referenced env variable is documented in .env.example;
- host ports are bound to loopback only;
- long-running services have healthchecks;
- stateful services use named volumes.
"""

import os
import re
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
TRINO_CATALOG_FILE = REPO_ROOT / "trino" / "etc" / "catalog" / "iceberg.properties"
ENV_EXAMPLE_FILE = REPO_ROOT / ".env.example"
MINIO_INIT_FILE = REPO_ROOT / "infrastructure" / "scripts" / "minio_init.sh"
MOCK_API_DIR = REPO_ROOT / "infrastructure" / "mock_api"
MOCK_API_DOCKERFILE = MOCK_API_DIR / "Dockerfile"
MOCK_API_REQUIREMENTS = MOCK_API_DIR / "requirements.txt"
AIRFLOW_DIR = REPO_ROOT / "airflow"
AIRFLOW_DOCKERFILE = REPO_ROOT / "infrastructure" / "airflow" / "Dockerfile"
AIRFLOW_TEST_SCRIPT = REPO_ROOT / "infrastructure" / "scripts" / "airflow_tests.sh"
AIRFLOW_IMAGE = "omni-retail/airflow:0.1.0"

# Environment documented for the ingestion pipeline (Phase 3 design spec §11).
EXPECTED_INGESTION_ENV_KEYS = {
    "S3_ENDPOINT_URL",
    "S3_ACCESS_KEY_ID",
    "S3_SECRET_ACCESS_KEY",
    "MOCK_API_BASE_URL",
    "MOCK_API_PORT",
    "MOCK_API_SEED",
}

# Trino/dbt configuration documented for Phase 5 (design spec §8).
EXPECTED_TRINO_ENV_KEYS = {"TRINO_HOST", "TRINO_PORT", "TRINO_CATALOG", "TRINO_USER"}

DBT_DIR = REPO_ROOT / "dbt"
DBT_STAGING_MODELS = {
    "stg_campaigns.sql",
    "stg_categories.sql",
    "stg_customers.sql",
    "stg_deliveries.sql",
    "stg_fx_rates.sql",
    "stg_order_items.sql",
    "stg_orders.sql",
    "stg_payments.sql",
    "stg_products.sql",
    "stg_shipments.sql",
}

# Airflow configuration documented for Phase 4 (design spec §11, ADR 0003).
EXPECTED_AIRFLOW_ENV_KEYS = {
    "AIRFLOW_UID",
    "AIRFLOW_FERNET_KEY",
    "AIRFLOW_WWW_USER",
    "AIRFLOW_WWW_PASSWORD",
    "AIRFLOW_WEBSERVER_PORT",
    "AIRFLOW_POSTGRES_USER",
    "AIRFLOW_POSTGRES_PASSWORD",
    "AIRFLOW_POSTGRES_DB",
}

FORBIDDEN_IMAGE_TAGS = {"latest", "stable", "edge", "nightly"}

EXPECTED_CORE_SERVICES = {
    "postgres",
    "minio",
    "minio-init",
    "polaris-postgres",
    "polaris-bootstrap",
    "polaris",
    "polaris-init",
    "trino",
    "mock-api",
}

EXPECTED_ORCHESTRATION_SERVICES = {
    "airflow-postgres",
    "airflow-init",
    "airflow-webserver",
    "airflow-scheduler",
}

# Services that run to completion and therefore must not have healthchecks.
ONE_SHOT_SERVICES = {
    "minio-init",
    "polaris-bootstrap",
    "polaris-init",
    "airflow-init",
    "clickhouse-init",
}


def _load_compose() -> dict[str, Any]:
    with COMPOSE_FILE.open(encoding="utf-8") as fh:
        return cast(dict[str, Any], yaml.safe_load(fh))


def _env_example_keys() -> set[str]:
    keys: set[str] = set()
    for line in ENV_EXAMPLE_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            keys.add(line.split("=", 1)[0])
    return keys


def test_compose_file_exists() -> None:
    assert COMPOSE_FILE.is_file(), "docker-compose.yml is missing"


def test_core_profile_contains_expected_services() -> None:
    compose = _load_compose()
    services = compose["services"]
    missing = EXPECTED_CORE_SERVICES - set(services)
    assert not missing, f"services missing from compose: {sorted(missing)}"
    for name in EXPECTED_CORE_SERVICES:
        assert "core" in services[name].get("profiles", []), f"{name} is not in the 'core' profile"


def test_images_are_pinned() -> None:
    services = _load_compose()["services"]
    unpinned: list[str] = []
    for name, cfg in services.items():
        image = cfg.get("image", "")
        tag = image.rsplit(":", 1)[-1] if ":" in image else ""
        if not tag or tag.lower() in FORBIDDEN_IMAGE_TAGS:
            unpinned.append(f"{name}: {image!r}")
    assert not unpinned, f"images must be pinned to meaningful versions: {unpinned}"


def test_env_references_are_documented_in_env_example() -> None:
    compose_text = COMPOSE_FILE.read_text(encoding="utf-8")
    referenced = set(re.findall(r"\$\{(\w+)(?::-[^}]*)?\}", compose_text))
    if TRINO_CATALOG_FILE.is_file():
        referenced |= set(
            re.findall(r"\$\{ENV:(\w+)\}", TRINO_CATALOG_FILE.read_text(encoding="utf-8"))
        )
    undocumented = referenced - _env_example_keys()
    assert not undocumented, (
        f"env vars used in config but missing from .env.example: {sorted(undocumented)}"
    )


def test_long_running_services_have_healthchecks() -> None:
    services = _load_compose()["services"]
    missing = [
        name
        for name, cfg in services.items()
        if name not in ONE_SHOT_SERVICES and "healthcheck" not in cfg
    ]
    assert not missing, f"long-running services without healthcheck: {missing}"


def test_host_ports_bind_to_loopback_only() -> None:
    services = _load_compose()["services"]
    public: list[str] = []
    for name, cfg in services.items():
        for entry in cfg.get("ports", []):
            host_part = str(entry).split(":", 1)[0]
            if host_part not in {"127.0.0.1", "localhost"}:
                public.append(f"{name}: {entry}")
    assert not public, f"ports must bind to 127.0.0.1 only: {public}"


def test_stateful_services_use_named_volumes() -> None:
    compose = _load_compose()
    volumes = set(compose.get("volumes", {}))
    required = {
        "postgres-data",
        "minio-data",
        "polaris-postgres-data",
        "airflow-metadata-data",
        "airflow-logs",
    }
    assert required <= volumes, f"missing named volumes: {sorted(required - volumes)}"
    services = compose["services"]
    assert any("postgres-data" in str(services["postgres"].get("volumes", [])) for _ in [0]), (
        "postgres must mount postgres-data"
    )
    assert any("minio-data" in str(services["minio"].get("volumes", [])) for _ in [0]), (
        "minio must mount minio-data"
    )
    assert any(
        "airflow-metadata-data" in str(services["airflow-postgres"].get("volumes", [])) for _ in [0]
    ), "airflow-postgres must mount airflow-metadata-data (ADR 0003)"


def test_trino_catalog_targets_polaris_rest_api() -> None:
    assert TRINO_CATALOG_FILE.is_file(), "trino/catalogs/iceberg.properties is missing"
    props = TRINO_CATALOG_FILE.read_text(encoding="utf-8")
    assert "connector.name=iceberg" in props
    assert "iceberg.catalog.type=rest" in props
    assert re.search(r"iceberg\.rest-catalog\.uri=\S+polaris:8181/api/catalog", props)
    assert "iceberg.rest-catalog.warehouse=lakehouse" in props


@pytest.mark.parametrize(
    ("service", "dependency", "condition"),
    [
        ("trino", "polaris-init", "service_completed_successfully"),
        ("polaris-init", "polaris", "service_healthy"),
        ("polaris-init", "minio-init", "service_completed_successfully"),
        ("polaris", "polaris-bootstrap", "service_completed_successfully"),
        ("minio-init", "minio", "service_healthy"),
        ("airflow-init", "airflow-postgres", "service_healthy"),
        ("airflow-webserver", "airflow-postgres", "service_healthy"),
        ("airflow-webserver", "airflow-init", "service_completed_successfully"),
        ("airflow-scheduler", "airflow-postgres", "service_healthy"),
        ("airflow-scheduler", "airflow-init", "service_completed_successfully"),
        ("airflow-scheduler", "airflow-webserver", "service_healthy"),
    ],
)
def test_service_dependencies_use_health_conditions(
    service: str, dependency: str, condition: str
) -> None:
    services = _load_compose()["services"]
    depends = services[service]["depends_on"][dependency]
    assert depends.get("condition") == condition, (
        f"{service} must wait for {dependency} ({condition})"
    )


def test_env_example_documents_ingestion_variables() -> None:
    missing = EXPECTED_INGESTION_ENV_KEYS - _env_example_keys()
    assert not missing, f"ingestion env vars missing from .env.example: {sorted(missing)}"


def test_orchestration_profile_contains_expected_services() -> None:
    services = _load_compose()["services"]
    missing = EXPECTED_ORCHESTRATION_SERVICES - set(services)
    assert not missing, f"orchestration services missing from compose: {sorted(missing)}"
    for name in EXPECTED_ORCHESTRATION_SERVICES:
        profiles = services[name].get("profiles", [])
        assert "orchestration" in profiles, f"{name} is not in the 'orchestration' profile"
        assert "core" not in profiles, f"{name} must not be in the 'core' profile"


def test_env_example_documents_airflow_variables() -> None:
    missing = EXPECTED_AIRFLOW_ENV_KEYS - _env_example_keys()
    assert not missing, f"airflow env vars missing from .env.example: {sorted(missing)}"


def test_airflow_services_use_custom_pinned_image() -> None:
    services = _load_compose()["services"]
    for name in ("airflow-init", "airflow-webserver", "airflow-scheduler"):
        assert services[name]["image"] == AIRFLOW_IMAGE, f"{name} must use {AIRFLOW_IMAGE}"
    assert services["airflow-init"]["build"]["dockerfile"].endswith(
        "infrastructure/airflow/Dockerfile"
    )


def test_airflow_postgres_has_no_host_ports() -> None:
    services = _load_compose()["services"]
    assert not services["airflow-postgres"].get("ports"), (
        "airflow-postgres metadata must stay Docker-network-local"
    )


def test_airflow_webserver_port_is_loopback_only() -> None:
    services = _load_compose()["services"]
    entry = str(services["airflow-webserver"]["ports"][0])
    assert entry.startswith("127.0.0.1:"), f"webserver port must bind loopback: {entry}"


def test_airflow_services_mount_code_readonly() -> None:
    services = _load_compose()["services"]
    for name in ("airflow-init", "airflow-webserver", "airflow-scheduler"):
        mounts = [str(m) for m in services[name].get("volumes", [])]
        for required in (
            "./airflow/dags:/opt/airflow/dags:ro",
            "./airflow/include:/opt/airflow/include:ro",
            "./airflow/tests:/opt/airflow/tests:ro",
        ):
            assert required in mounts, f"{name} must mount {required}"


def test_airflow_uses_local_executor_and_paused_dags() -> None:
    services = _load_compose()["services"]
    for name in ("airflow-init", "airflow-webserver", "airflow-scheduler"):
        env = services[name]["environment"]
        assert env["AIRFLOW__CORE__EXECUTOR"] == "LocalExecutor", f"{name}: LocalExecutor"
        assert env["AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION"] == "True", (
            f"{name}: new DAGs must start paused"
        )
        assert env["AIRFLOW__CORE__LOAD_EXAMPLES"] == "False", f"{name}: no example DAGs"


def test_airflow_metadata_db_is_external_to_the_source_db() -> None:
    services = _load_compose()["services"]
    conn = services["airflow-webserver"]["environment"]["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
    assert "airflow-postgres:5432" in conn, "metadata DB must be the dedicated instance"
    assert "postgresql+psycopg2://" in conn


def test_airflow_ingestion_env_uses_docker_network_addresses() -> None:
    env = _load_compose()["services"]["airflow-scheduler"]["environment"]
    assert env["S3_ENDPOINT_URL"] == "http://minio:9000"
    assert env["MOCK_API_BASE_URL"] == "http://mock-api:9002"
    assert env["POSTGRES_HOST"] == "postgres"


def test_airflow_dockerfile_pinned_base_and_nonroot() -> None:
    assert AIRFLOW_DOCKERFILE.is_file(), "airflow Dockerfile is missing"
    text = AIRFLOW_DOCKERFILE.read_text(encoding="utf-8")
    assert "FROM apache/airflow:2.11.2-python3.12" in text, "base image must be pinned"
    assert "ghcr.io/astral-sh/uv:" in text, "uv must be copied from a pinned tag"
    assert "uv export --frozen --no-dev" in text, "versions must come from the lockfile"
    assert text.rstrip().endswith("USER airflow"), "image must end as the airflow user"


def test_airflow_dockerfile_context_whitelist_is_minimal() -> None:
    dockerignore = REPO_ROOT / ".dockerignore"
    assert dockerignore.is_file(), "root .dockerignore is missing (Airflow build context)"
    entries = [
        line.strip()
        for line in dockerignore.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]
    assert entries[0] == "*", "dockerignore must use a whitelist"
    for required in ("!pyproject.toml", "!uv.lock", "!src/**"):
        assert required in entries, f"dockerignore must whitelist {required}"


def test_airflow_tests_script_is_executable_harness() -> None:
    assert AIRFLOW_TEST_SCRIPT.is_file(), "airflow_tests.sh is missing"
    text = AIRFLOW_TEST_SCRIPT.read_text(encoding="utf-8")
    assert "set -euo pipefail" in text
    assert "python -m pytest /opt/airflow/tests" in text
    assert "list-import-errors" in text
    assert os.access(AIRFLOW_TEST_SCRIPT, os.X_OK), "airflow_tests.sh must be executable"


def test_dag_directory_contains_only_expected_dags() -> None:
    dags_dir = AIRFLOW_DIR / "dags"
    assert dags_dir.is_dir(), "airflow/dags is missing"
    dag_files = sorted(path.name for path in dags_dir.glob("*.py"))
    expected = [
        "ingest_delivery_api.py",
        "ingest_fx_api.py",
        "ingest_marketing_api.py",
        "ingest_postgres_snapshot.py",
        "ingest_supplier_files.py",
        "load_bronze.py",
        "publish_serving.py",
        "transform_lakehouse.py",
    ]
    assert dag_files == expected, f"unexpected DAG files: {dag_files}"


def test_minio_init_creates_least_privilege_ingestion_user() -> None:
    script = MINIO_INIT_FILE.read_text(encoding="utf-8")

    assert "mc admin user add" in script
    assert "mc admin policy attach local omni-ingestion-rw" in script
    # the policy must be scoped to ingestion buckets only, without admin rights
    for bucket in ("landing", "archive", "rejected"):
        assert f"arn:aws:s3:::{bucket}" in script
    assert "s3:PutObject" in script
    assert "Administrator" not in script


def test_mock_api_build_context_exists_with_pinned_base_image() -> None:
    assert MOCK_API_DOCKERFILE.is_file(), "mock-api Dockerfile is missing"
    from_lines = [
        line.strip()
        for line in MOCK_API_DOCKERFILE.read_text(encoding="utf-8").splitlines()
        if line.strip().upper().startswith("FROM ")
    ]
    assert from_lines, "Dockerfile must contain a FROM instruction"
    for line in from_lines:
        tag = line.split()[-1].rsplit(":", 1)[-1]
        assert ":" in line.split()[-1], f"base image must carry a tag: {line}"
        assert tag.lower() not in FORBIDDEN_IMAGE_TAGS, f"unpinned base image: {line}"


def test_mock_api_requirements_are_version_pinned() -> None:
    assert MOCK_API_REQUIREMENTS.is_file(), "mock-api requirements.txt is missing"
    for line in MOCK_API_REQUIREMENTS.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        assert any(marker in line for marker in ("==", ">=", "<=", "~=")), (
            f"dependency without a version constraint: {line}"
        )


def test_mock_api_healthcheck_targets_healthz() -> None:
    services = _load_compose()["services"]
    healthcheck = services["mock-api"]["healthcheck"]
    assert "/healthz" in str(healthcheck["test"])


def test_mock_api_runs_non_root_user() -> None:
    dockerfile = MOCK_API_DOCKERFILE.read_text(encoding="utf-8")
    assert "USER " in dockerfile, "mock-api container must run as a non-root user"


def test_env_example_documents_trino_variables() -> None:
    missing = EXPECTED_TRINO_ENV_KEYS - _env_example_keys()
    assert not missing, f"trino env vars missing from .env.example: {sorted(missing)}"


def test_dbt_profiles_are_env_driven_and_secret_free() -> None:
    profiles = yaml.safe_load((DBT_DIR / "profiles.yml").read_text(encoding="utf-8"))
    output = profiles["omni_retail"]["outputs"]["dev"]
    assert output["type"] == "trino"
    assert output["http_scheme"] == "http"
    for forbidden in ("password", "key_file", "access_token", "credentials"):
        assert forbidden not in output, f"profiles.yml must not contain {forbidden!r}"
    for env_driven in ("host", "user", "catalog", "schema"):
        assert "env_var" in str(output[env_driven]), f"{env_driven} must come from env_var"


def test_dbt_project_and_staging_skeleton_exist() -> None:
    assert (DBT_DIR / "dbt_project.yml").is_file()
    assert (DBT_DIR / "profiles.yml").is_file()
    assert (DBT_DIR / "models" / "staging" / "sources.yml").is_file()
    models = {path.name for path in (DBT_DIR / "models" / "staging").glob("stg_*.sql")}
    assert models == DBT_STAGING_MODELS
