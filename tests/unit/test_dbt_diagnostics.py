"""Hermetic checks for isolated dbt integration diagnostics."""

# pyright: reportMissingImports=false

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

import pytest

import integration.dbt_diagnostics as dbt_diagnostics


def run_results_payload(results: list[dict[str, object]]) -> dict[str, object]:
    return {
        "metadata": {"invocation_id": "invocation-123"},
        "results": results,
        "elapsed_time": 1.0,
        "args": {},
    }


def write_run_results(path: Path, results: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(run_results_payload(results)), encoding="utf-8")


def test_parse_dbt_results_extracts_terminal_error_and_ignores_skipped(tmp_path: Path) -> None:
    path = tmp_path / "run_results.json"
    write_run_results(
        path,
        [
            {
                "status": "success",
                "unique_id": "model.omni_retail.stg_orders",
                "message": "CREATE VIEW",
            },
            {
                "status": "error",
                "unique_id": "model.omni_retail.mart_delivery_performance",
                "message": (
                    "Database Error in model mart_delivery_performance\n"
                    "TrinoExternalError(type=EXTERNAL, name=ICEBERG_CATALOG_ERROR, "
                    'message="Failed to create transaction", '
                    "query_id=20261007_122901_15437_tc2gx)"
                ),
            },
            {
                "status": "skipped",
                "unique_id": "test.omni_retail.delivery_reconciliation",
                "message": None,
            },
        ],
    )

    parsed = dbt_diagnostics.parse_dbt_run_results(path)

    assert parsed.invocation_id == "invocation-123"
    assert parsed.result_count == 3
    assert dict(parsed.status_counts) == {"error": 1, "skipped": 1, "success": 1}
    assert parsed.skipped_count == 1
    assert len(parsed.failures) == 1
    failure = parsed.failures[0]
    assert failure.unique_id == "model.omni_retail.mart_delivery_performance"
    assert failure.adapter_error_class == "TrinoExternalError"
    assert failure.adapter_error_type == "EXTERNAL"
    assert failure.adapter_error_name == "ICEBERG_CATALOG_ERROR"
    assert failure.adapter_error_message == "Failed to create transaction"
    assert failure.query_ids == ("20261007_122901_15437_tc2gx",)


def test_parse_dbt_results_preserves_failed_data_test_as_terminal(tmp_path: Path) -> None:
    path = tmp_path / "run_results.json"
    write_run_results(
        path,
        [
            {
                "status": "fail",
                "unique_id": "test.omni_retail.recon_orders_vs_payments",
                "message": "Got 2 results, configured to fail if != 0",
                "failures": 2,
            }
        ],
    )

    parsed = dbt_diagnostics.parse_dbt_run_results(path)

    assert len(parsed.failures) == 1
    assert parsed.failures[0].status == "fail"
    assert parsed.failures[0].adapter_error_name is None
    assert parsed.failures[0].query_ids == ()


@pytest.mark.parametrize(
    "payload, message",
    [
        ({}, "metadata must be an object"),
        ({"metadata": {"invocation_id": "id"}}, "results must be an array"),
        (
            {"metadata": {"invocation_id": "id"}, "results": [{}]},
            "results[0].status must be a non-empty string",
        ),
    ],
)
def test_parse_dbt_results_fails_closed_on_incomplete_artifact(
    tmp_path: Path,
    payload: dict[str, object],
    message: str,
) -> None:
    path = tmp_path / "run_results.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(dbt_diagnostics.DbtArtifactError, match=re.escape(message)):
        dbt_diagnostics.parse_dbt_run_results(path)


def test_parse_dbt_results_fails_closed_when_artifact_is_missing(tmp_path: Path) -> None:
    with pytest.raises(dbt_diagnostics.DbtArtifactError, match="is missing"):
        dbt_diagnostics.parse_dbt_run_results(tmp_path / "missing.json")


def test_run_dbt_build_isolates_paths_and_disables_telemetry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_commands: list[tuple[str, ...]] = []

    def successful_run(
        command: tuple[str, ...],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        observed_commands.append(command)
        assert command[:6] == (
            "uv",
            "run",
            "python",
            "-m",
            "omni_retail.lakehouse.dbt_cli",
            "build",
        )
        env = kwargs["env"]
        assert isinstance(env, dict)
        assert env["DBT_SEND_ANONYMOUS_USAGE_STATS"] == "false"
        target_path = Path(command[command.index("--target-path") + 1])
        write_run_results(
            target_path / "run_results.json",
            [
                {
                    "status": "success",
                    "unique_id": "model.omni_retail.stg_orders",
                    "message": "CREATE VIEW",
                }
            ],
        )
        return subprocess.CompletedProcess(command, 0, stdout="dbt stdout", stderr="")

    monkeypatch.setattr("integration.dbt_diagnostics.subprocess.run", successful_run)

    invocation = dbt_diagnostics.run_dbt_build(
        schema_env={"DBT_GOLD_SCHEMA": "it_123_gold"},
        schema_names=("it_123_bronze", "it_123_silver", "it_123_gold", "it_123_analytics"),
        run_dir=tmp_path / "dbt-first",
        label="first",
    )

    assert len(observed_commands) == 1
    assert invocation.target_path == tmp_path / "dbt-first" / "target"
    assert invocation.log_path == tmp_path / "dbt-first" / "logs"
    assert invocation.stdout_path.read_text(encoding="utf-8") == "dbt stdout"
    assert invocation.results is not None
    assert invocation.results.result_count == 1


def test_run_dbt_build_reports_structured_failure_without_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def failed_run(
        command: tuple[str, ...],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        target_path = Path(command[command.index("--target-path") + 1])
        write_run_results(
            target_path / "run_results.json",
            [
                {
                    "status": "error",
                    "unique_id": "model.omni_retail.mart_delivery_performance",
                    "message": (
                        "TrinoExternalError(type=EXTERNAL, name=ICEBERG_CATALOG_ERROR, "
                        'message="Failed to create transaction", query_id=query_123)'
                    ),
                },
                {
                    "status": "skipped",
                    "unique_id": "test.omni_retail.delivery_reconciliation",
                    "message": None,
                },
            ],
        )
        return subprocess.CompletedProcess(
            command,
            1,
            stdout="first line\nlast stdout line",
            stderr="last stderr line",
        )

    monkeypatch.setattr("integration.dbt_diagnostics.subprocess.run", failed_run)

    with pytest.raises(dbt_diagnostics.DbtBuildFailure) as raised:
        dbt_diagnostics.run_dbt_build(
            schema_env={},
            schema_names=("it_123_bronze", "it_123_silver"),
            run_dir=tmp_path / "dbt-second",
            label="second",
        )

    assert calls == 1
    message = str(raised.value)
    assert "label=second returncode=1" in message
    assert "invocation_id=invocation-123" in message
    assert "model.omni_retail.mart_delivery_performance" in message
    assert "adapter_name=ICEBERG_CATALOG_ERROR" in message
    assert "query_ids=('query_123',)" in message
    assert "skipped_count=1" in message
    assert "last stdout line" in message
    assert "last stderr line" in message


@pytest.mark.parametrize("returncode", [0, 1])
def test_run_dbt_build_fails_closed_without_results_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    returncode: int,
) -> None:
    calls: list[tuple[str, ...]] = []

    def run_without_artifact(
        command: tuple[str, ...],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, returncode, stdout="", stderr="")

    monkeypatch.setattr("integration.dbt_diagnostics.subprocess.run", run_without_artifact)

    with pytest.raises(dbt_diagnostics.DbtBuildFailure, match="artifact_error=.*is missing"):
        dbt_diagnostics.run_dbt_build(
            schema_env={},
            schema_names=("it_123_bronze",),
            run_dir=tmp_path / "dbt",
            label="first",
        )

    assert len(calls) == 1
