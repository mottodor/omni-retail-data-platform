"""CDC runtime configuration and readiness tests."""

# pyright: reportMissingImports=false

import os
import time
from pathlib import Path

import pytest

from omni_retail.streaming.cdc.cli import readiness_is_fresh
from omni_retail.streaming.cdc.config import CdcConfig, CdcConfigError


def test_config_builds_the_exact_three_topic_allow_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for key in list(os.environ):
        if key.startswith("CDC_") or key == "KAFKA_BOOTSTRAP_SERVERS":
            monkeypatch.delenv(key, raising=False)
    config = CdcConfig.from_env()

    assert config.topic_tables == {
        "omni.oltp.public.customers": "customers",
        "omni.oltp.public.orders": "orders",
        "omni.oltp.public.payments": "payments",
    }
    assert config.group_id.endswith("-v1")
    assert config.batch_size == 500
    assert config.connect_url == "http://127.0.0.1:8083"
    assert config.boundary_stability_seconds == 10.0
    assert config.boundary_timeout_seconds == 300.0
    assert config.boundary_request_timeout_seconds == 10.0


def test_config_rejects_invalid_batch_and_namespace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CDC_BATCH_SIZE", "0")
    with pytest.raises(CdcConfigError, match="greater than zero"):
        CdcConfig.from_env()

    monkeypatch.setenv("CDC_BATCH_SIZE", "501")
    with pytest.raises(CdcConfigError, match="must not exceed 500"):
        CdcConfig.from_env()

    monkeypatch.setenv("CDC_BATCH_SIZE", "10")
    monkeypatch.setenv("CDC_TOPIC_PREFIX", "changed.namespace")
    with pytest.raises(CdcConfigError, match="must remain"):
        CdcConfig.from_env()


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("CDC_BOUNDARY_STABILITY_SECONDS", "0", "greater than zero"),
        ("CDC_BOUNDARY_STABILITY_SECONDS", "nan", "finite number"),
        ("CDC_BOUNDARY_TIMEOUT_SECONDS", "not-a-number", "must be numeric"),
        ("CDC_BOUNDARY_REQUEST_TIMEOUT_SECONDS", "-1", "greater than zero"),
        ("KAFKA_CONNECT_URL", "localhost:8083", "absolute HTTP"),
        ("KAFKA_CONNECT_URL", "http://user:secret@localhost:8083", "credentials"),
    ],
)
def test_config_rejects_invalid_boundary_settings(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
    message: str,
) -> None:
    monkeypatch.setenv(name, value)
    with pytest.raises(CdcConfigError, match=message):
        CdcConfig.from_env()


def test_readiness_requires_a_recent_heartbeat(tmp_path: Path) -> None:
    path = tmp_path / "ready"
    assert readiness_is_fresh(path, max_age_seconds=10) is False

    path.write_text("ready", encoding="utf-8")
    assert readiness_is_fresh(path, max_age_seconds=10) is True

    old = time.time() - 30
    os.utime(path, (old, old))
    assert readiness_is_fresh(path, max_age_seconds=10) is False
