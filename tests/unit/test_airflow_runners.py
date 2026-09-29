"""Unit tests for environment-resolved Airflow runner isolation boundaries."""

# pyright: reportMissingImports=false, reportMissingTypeStubs=false

import pytest
from include import runners

from fakes.storage import FakeStorage
from omni_retail.lakehouse.bronze.specs import TABLES


class FakeClosableExecutor:
    def close(self) -> None:
        pass


def test_bronze_runner_uses_custom_schema_and_injected_storage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = FakeStorage()
    calls: list[tuple[object, str]] = []

    def fake_load_new(
        supplied_storage: object,
        executor: object,
        spec: object,
        *,
        schema: str,
    ) -> list[object]:
        calls.append((supplied_storage, schema))
        return []

    monkeypatch.setattr(runners, "DbapiTrinoExecutor", lambda config: FakeClosableExecutor())
    monkeypatch.setattr(runners, "load_new", fake_load_new)

    summary = runners.run_bronze_load(storage=storage, schema="it_runner_bronze")

    assert summary["sources"] == len(TABLES)
    assert len(calls) == len(TABLES)
    assert all(supplied is storage for supplied, _schema in calls)
    assert {schema for _storage, schema in calls} == {"it_runner_bronze"}


def test_bronze_runner_keeps_no_argument_production_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = FakeStorage()
    schemas: list[str] = []

    def fake_load_new(
        supplied_storage: object,
        executor: object,
        spec: object,
        *,
        schema: str,
    ) -> list[object]:
        assert supplied_storage is storage
        schemas.append(schema)
        return []

    monkeypatch.delenv("ICEBERG_BRONZE_SCHEMA", raising=False)
    monkeypatch.setenv("S3_ACCESS_KEY_ID", "test-key")
    monkeypatch.setenv("S3_SECRET_ACCESS_KEY", "test-secret")
    monkeypatch.setattr(runners, "BotoObjectStorage", lambda config: storage)
    monkeypatch.setattr(runners, "DbapiTrinoExecutor", lambda config: FakeClosableExecutor())
    monkeypatch.setattr(runners, "load_new", fake_load_new)

    runners.run_bronze_load()

    assert set(schemas) == {"bronze"}


def test_dbt_schema_env_resolves_defaults_and_custom_values() -> None:
    assert runners.resolve_dbt_schema_env({}) == {
        "DBT_BRONZE_SCHEMA": "bronze",
        "DBT_SILVER_SCHEMA": "silver",
        "DBT_GOLD_SCHEMA": "gold",
        "DBT_ANALYTICS_SCHEMA": "analytics",
    }
    assert (
        runners.resolve_dbt_schema_env(
            {
                "DBT_BRONZE_SCHEMA": "it_a_bronze",
                "DBT_SILVER_SCHEMA": "it_a_silver",
                "DBT_GOLD_SCHEMA": "it_a_gold",
                "DBT_ANALYTICS_SCHEMA": "it_a_analytics",
            }
        )["DBT_GOLD_SCHEMA"]
        == "it_a_gold"
    )


def test_dbt_schema_env_rejects_invalid_identifier() -> None:
    with pytest.raises(ValueError, match="invalid schema name"):
        runners.resolve_dbt_schema_env({"DBT_GOLD_SCHEMA": "gold; drop schema bronze"})


def test_bronze_runner_rejects_invalid_schema_before_connecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_constructor(config: object) -> object:
        pytest.fail("executor must not be constructed")

    monkeypatch.setattr(runners, "DbapiTrinoExecutor", fail_constructor)

    with pytest.raises(ValueError, match="invalid schema name"):
        runners.run_bronze_load(storage=FakeStorage(), schema="bronze; drop schema gold")
