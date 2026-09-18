"""Unit tests for the pure logic of the Polaris catalog initializer."""

import importlib.util
from pathlib import Path
from types import ModuleType

SCRIPT = Path(__file__).resolve().parents[1] / "infrastructure" / "scripts" / "polaris_init.py"


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("polaris_init", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_build_catalog_payload_shape() -> None:
    module = _load_module()
    payload = module.build_catalog_payload(
        catalog_name="lakehouse",
        base_location="s3://lakehouse/",
        storage={
            "endpoint": "http://minio:9000",
            "region": "us-east-1",
        },
    )
    catalog = payload["catalog"]
    assert catalog["name"] == "lakehouse"
    assert catalog["type"] == "INTERNAL"
    assert catalog["properties"]["default-base-location"] == "s3://lakehouse/"
    storage = catalog["storageConfigInfo"]
    assert storage["storageType"] == "S3"
    assert storage["endpoint"] == "http://minio:9000"
    assert storage["allowedLocations"] == ["s3://lakehouse/"]
    assert storage["pathStyleAccess"] is True


def test_build_catalog_payload_has_no_inline_credentials() -> None:
    """S3 credentials must come from Polaris container env, not the payload."""
    module = _load_module()
    payload = module.build_catalog_payload(
        catalog_name="lakehouse",
        base_location="s3://lakehouse/",
        storage={
            "endpoint": "http://minio:9000",
            "region": "us-east-1",
        },
    )
    rendered = str(payload)
    assert "accessKey" not in rendered
    assert "secretKey" not in rendered


def test_catalog_exists_decision_is_idempotent() -> None:
    module = _load_module()
    assert module.decide_action(catalog_exists=True) == "skip"
    assert module.decide_action(catalog_exists=False) == "create"


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
    """CATALOG_MANAGE_CONTENT covers TABLE_DROP/VIEW_DROP inheritance and the
    TABLE_WRITE_DATA needed for Trino's purge-drops."""
    module = _load_module()
    assert module.REQUIRED_CATALOG_PRIVILEGES == ("CATALOG_MANAGE_CONTENT",)
