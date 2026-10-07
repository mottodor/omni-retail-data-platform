"""Contract guards for host-side Makefile environment handling."""

import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
MAKEFILE = REPO_ROOT / "Makefile"
HOST_RUNNER = REPO_ROOT / "infrastructure/scripts/run_host_command.sh"
LOCALHOSTS = "127.0.0.1,localhost"
RUNNER_COMMAND = "bash infrastructure/scripts/run_host_command.sh"

HOST_SIDE_SERVICE_TARGETS = (
    "generate-oltp",
    "mutate-oltp",
    "seed-supplier-files",
    "ingest-files",
    "ingest-api",
    "bronze-load",
    "bronze-rebuild",
    "dbt-build",
    "dbt-test",
    "integration",
    "streaming-status",
    "serving-publish",
    "serving-rebuild",
    "serving-benchmark",
)


def _render_target(target: str) -> str:
    result = subprocess.run(
        ["make", "--no-print-directory", "--just-print", target],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def test_local_service_environment_has_one_shared_runner() -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")
    runner = HOST_RUNNER.read_text(encoding="utf-8")

    assert makefile.count("HOST_RUN :=") == 1
    assert runner.count(LOCALHOSTS) == 2
    assert LOCALHOSTS not in makefile


@pytest.mark.parametrize("target", HOST_SIDE_SERVICE_TARGETS)
def test_host_side_service_targets_render_shared_environment_runner(target: str) -> None:
    recipe = _render_target(target)
    assert RUNNER_COMMAND in recipe, f"{target} does not use the shared host runner"


def test_offline_target_does_not_receive_local_service_environment() -> None:
    recipe = _render_target("lint")
    assert RUNNER_COMMAND not in recipe


def test_streaming_status_uses_typed_health_cli_and_propagates_its_exit_code() -> None:
    recipe = _render_target("streaming-status")

    assert "python -m omni_retail.streaming.cdc status" in recipe
    assert "/connectors/omni-postgres-cdc/config" not in recipe
    assert "kafka-consumer-groups.sh" not in recipe


def test_reset_explicitly_couples_core_and_streaming_without_bi_or_airflow() -> None:
    recipe = _render_target("reset")

    assert "--profile core --profile streaming" in recipe
    assert "down -v" in recipe
    assert "Kafka/Connect transport state" in recipe
    assert "BI and Airflow metadata volumes are preserved" in recipe
    assert "--profile bi" not in recipe
    assert "--profile orchestration" not in recipe


@pytest.mark.parametrize(
    ("lower", "upper", "expected_lower", "expected_upper"),
    [
        ("", "", LOCALHOSTS, LOCALHOSTS),
        (
            "lower.internal",
            "upper.internal",
            f"{LOCALHOSTS},lower.internal",
            f"{LOCALHOSTS},upper.internal",
        ),
    ],
)
def test_host_runner_preserves_proxy_exclusions_and_explicit_project(
    lower: str,
    upper: str,
    expected_lower: str,
    expected_upper: str,
) -> None:
    env = {
        **os.environ,
        "COMPOSE_PROJECT_NAME": "isolated-validation",
        "no_proxy": lower,
        "NO_PROXY": upper,
    }
    result = subprocess.run(
        [
            "bash",
            str(HOST_RUNNER),
            "bash",
            "-c",
            'printf "%s\\n%s\\n%s\\n" "$no_proxy" "$NO_PROXY" "$COMPOSE_PROJECT_NAME"',
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.stdout.splitlines() == [
        expected_lower,
        expected_upper,
        "isolated-validation",
    ]
