"""Unit tests for the PostgreSQL source config parsing."""

import pytest

from omni_retail.ingestion.postgres_snapshot.config import PostgresSourceConfig


def test_from_env_reads_all_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_HOST", "postgres")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_USER", "oltp_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "oltp_pass")
    monkeypatch.setenv("POSTGRES_DB", "omni_oltp")

    config = PostgresSourceConfig.from_env()

    assert config.host == "postgres"
    assert config.port == 5432
    assert config.dbname == "omni_oltp"
    assert "user=oltp_user" in config.conninfo()
    assert "password=oltp_pass" in config.conninfo()


def test_from_env_defaults_host_and_port(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_USER", "u")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p")
    monkeypatch.setenv("POSTGRES_DB", "d")
    monkeypatch.delenv("POSTGRES_HOST", raising=False)
    monkeypatch.delenv("POSTGRES_PORT", raising=False)

    config = PostgresSourceConfig.from_env()

    assert config.host == "localhost"
    assert config.port == 5432


def test_from_env_requires_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ValueError, match="missing required environment variables"):
        PostgresSourceConfig.from_env()


def test_repr_masks_the_password(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_USER", "u")
    monkeypatch.setenv("POSTGRES_PASSWORD", "s3cret")
    monkeypatch.setenv("POSTGRES_DB", "d")

    config = PostgresSourceConfig.from_env()

    assert "s3cret" not in repr(config)
    assert "***masked***" in repr(config)
