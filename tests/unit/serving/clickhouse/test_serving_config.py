"""Unit tests for the serving ClickHouse config (env parsing only)."""

import pytest

from omni_retail.serving.clickhouse.config import ClickHouseConfig

ENV_KEYS = (
    "CLICKHOUSE_HOST",
    "CLICKHOUSE_PORT",
    "CLICKHOUSE_DB",
    "CLICKHOUSE_PUBLISHER_USER",
    "CLICKHOUSE_PUBLISHER_PASSWORD",
    "CLICKHOUSE_READER_USER",
    "CLICKHOUSE_READER_PASSWORD",
)


def test_defaults_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ENV_KEYS:
        monkeypatch.delenv(name, raising=False)
    assert ClickHouseConfig.from_env() == ClickHouseConfig()


def test_publisher_account_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLICKHOUSE_HOST", "clickhouse")
    monkeypatch.setenv("CLICKHOUSE_PORT", "8123")
    monkeypatch.setenv("CLICKHOUSE_DB", "analytics")
    monkeypatch.setenv("CLICKHOUSE_PUBLISHER_USER", "omni_publisher")
    monkeypatch.setenv("CLICKHOUSE_PUBLISHER_PASSWORD", "secret")
    config = ClickHouseConfig.from_env()
    assert config.host == "clickhouse"
    assert config.port == 8123
    assert config.database == "analytics"
    assert config.user == "omni_publisher"
    assert config.password == "secret"


def test_reader_account_inherits_connection_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLICKHOUSE_HOST", "10.0.0.5")
    monkeypatch.setenv("CLICKHOUSE_PORT", "18123")
    monkeypatch.setenv("CLICKHOUSE_READER_USER", "superset_reader")
    monkeypatch.setenv("CLICKHOUSE_READER_PASSWORD", "reader-secret")
    reader = ClickHouseConfig.reader_from_env()
    assert (reader.host, reader.port) == ("10.0.0.5", 18123)
    assert (reader.user, reader.password) == ("superset_reader", "reader-secret")


def test_reader_defaults_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ENV_KEYS:
        monkeypatch.delenv(name, raising=False)
    reader = ClickHouseConfig.reader_from_env()
    assert reader.user == "superset_reader"
    assert reader.password == ""
    assert reader.database == "analytics"
