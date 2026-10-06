"""Unit tests for environment-resolved Airflow runner isolation boundaries."""

# pyright: reportMissingImports=false, reportMissingTypeStubs=false

import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest
from include import runners

from fakes.storage import FakeStorage
from omni_retail.lakehouse.bronze.specs import TABLES


class FakeClosableExecutor:
    def close(self) -> None:
        pass


def valid_boundary() -> dict[str, object]:
    return {
        topic: {"partition": 0, "offset_exclusive": (index + 1) * 10}
        for index, topic in enumerate(reversed(runners.CDC_BOUNDARY_TOPICS))
    }


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


def test_cdc_boundary_serialization_is_strict_and_deterministic() -> None:
    serialized = runners.serialize_cdc_boundary(valid_boundary())

    assert serialized == json.dumps(
        {"cdc_boundary": runners.validate_cdc_boundary(valid_boundary())},
        sort_keys=True,
        separators=(",", ":"),
    )
    assert list(json.loads(serialized)["cdc_boundary"]) == sorted(runners.CDC_BOUNDARY_TOPICS)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda value: value.pop(runners.CDC_BOUNDARY_TOPICS[0]), "topics must match exactly"),
        (
            lambda value: value.__setitem__(
                "omni.oltp.public.unexpected", {"partition": 0, "offset_exclusive": 1}
            ),
            "topics must match exactly",
        ),
        (
            lambda value: value.__setitem__(
                runners.CDC_BOUNDARY_TOPICS[0], {"partition": 1, "offset_exclusive": 1}
            ),
            "partition",
        ),
        (
            lambda value: value.__setitem__(
                runners.CDC_BOUNDARY_TOPICS[0], {"partition": 0, "offset_exclusive": -1}
            ),
            "non-negative integer",
        ),
        (
            lambda value: value.__setitem__(
                runners.CDC_BOUNDARY_TOPICS[0], {"partition": 0, "offset_exclusive": True}
            ),
            "non-negative integer",
        ),
        (
            lambda value: value.__setitem__(
                runners.CDC_BOUNDARY_TOPICS[0],
                {"partition": 0, "offset_exclusive": 1, "extra": 2},
            ),
            "contain exactly",
        ),
    ],
)
def test_cdc_boundary_validation_rejects_invalid_contract_before_dbt(
    mutate: Callable[[dict[str, object]], object],
    message: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    boundary = valid_boundary()
    mutate(boundary)
    monkeypatch.setattr(
        "include.runners.subprocess.run",
        lambda *args, **kwargs: pytest.fail("dbt subprocess must not start"),
    )

    with pytest.raises(runners.CdcBoundaryValidationError, match=message):
        runners.run_dbt_build(boundary)


def test_dbt_build_passes_boundary_as_one_json_argument(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[list[str]] = []

    def successful_dbt(
        command: list[str],
        *,
        capture_output: bool,
        text: bool,
        check: bool,
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        assert capture_output is True
        assert text is True
        assert check is False
        assert env["DBT_SEND_ANONYMOUS_USAGE_STATS"] == "false"
        assert command[:4] == [
            sys.executable,
            "-c",
            runners.DBT_CLI_ENTRYPOINT,
            "build",
        ]
        observed.append(command)
        target_path = Path(command[command.index("--target-path") + 1])
        target_path.mkdir(parents=True)
        (target_path / "run_results.json").write_text(
            json.dumps({"elapsed_time": 1.25, "results": [{"status": "success"}]}),
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr("include.runners.tempfile.mkdtemp", lambda prefix: str(tmp_path / prefix))
    monkeypatch.setattr("include.runners.subprocess.run", successful_dbt)

    summary = runners.run_dbt_build(valid_boundary())

    assert summary["status_counts"] == {"success": 1}
    command = observed[0]
    assert command.count("--vars") == 1
    vars_index = command.index("--vars")
    assert json.loads(command[vars_index + 1]) == {
        "cdc_boundary": runners.validate_cdc_boundary(valid_boundary())
    }


def test_dbt_build_keeps_manual_unbounded_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[list[str]] = []

    def successful_dbt(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        observed.append(command)
        target_path = Path(command[command.index("--target-path") + 1])
        target_path.mkdir(parents=True)
        (target_path / "run_results.json").write_text(
            '{"elapsed_time": 0, "results": []}', encoding="utf-8"
        )
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr("include.runners.tempfile.mkdtemp", lambda prefix: str(tmp_path / prefix))
    monkeypatch.setattr("include.runners.subprocess.run", successful_dbt)

    runners.run_dbt_build()

    assert "--vars" not in observed[0]
