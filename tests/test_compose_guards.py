"""Guards for docker-compose configuration quality.

Enforces AGENTS.md infrastructure rules:
- pinned image versions (no latest/stable/edge);
- every referenced env variable is documented in .env.example;
- host ports are bound to loopback only;
- long-running services have healthchecks;
- stateful services use named volumes.
"""

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

# Environment documented for the ingestion pipeline (Phase 3 design spec §11).
EXPECTED_INGESTION_ENV_KEYS = {
    "S3_ENDPOINT_URL",
    "S3_ACCESS_KEY_ID",
    "S3_SECRET_ACCESS_KEY",
    "MOCK_API_BASE_URL",
    "MOCK_API_PORT",
    "MOCK_API_SEED",
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

# Services that run to completion and therefore must not have healthchecks.
ONE_SHOT_SERVICES = {"minio-init", "polaris-bootstrap", "polaris-init"}


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
    required = {"postgres-data", "minio-data", "polaris-postgres-data"}
    assert required <= volumes, f"missing named volumes: {sorted(required - volumes)}"
    services = compose["services"]
    assert any("postgres-data" in str(services["postgres"].get("volumes", [])) for _ in [0]), (
        "postgres must mount postgres-data"
    )
    assert any("minio-data" in str(services["minio"].get("volumes", [])) for _ in [0]), (
        "minio must mount minio-data"
    )


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
