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
    "iceberg-snapshot-plan",
    "iceberg-snapshot-expire",
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


def test_up_builds_repository_owned_minio_images_before_start() -> None:
    recipe = _render_target("up")
    build = "docker compose build minio minio-init"
    start = "docker compose --profile core up -d --wait"

    assert build in recipe
    assert start in recipe
    assert recipe.index(build) < recipe.index(start)


def test_snapshot_maintenance_targets_separate_preview_from_explicit_apply() -> None:
    plan = _render_target("iceberg-snapshot-plan")
    expire = _render_target("iceberg-snapshot-expire")

    assert "lakehouse.maintenance plan" in plan
    assert "--confirm" not in plan
    assert "lakehouse.maintenance expire --confirm" in expire
    assert "snapshot expiration is irreversible" in expire


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
    tmp_path: Path,
    lower: str,
    upper: str,
    expected_lower: str,
    expected_upper: str,
) -> None:
    # Exercise the runner in a clean-clone-like layout instead of depending on
    # the developer's gitignored .env (which is intentionally absent in CI).
    sandbox_runner = tmp_path / "infrastructure" / "scripts" / HOST_RUNNER.name
    sandbox_runner.parent.mkdir(parents=True)
    sandbox_runner.write_text(HOST_RUNNER.read_text(encoding="utf-8"), encoding="utf-8")
    (tmp_path / ".env").write_text("COMPOSE_PROJECT_NAME=from-dotenv\n", encoding="utf-8")

    env = {
        **os.environ,
        "COMPOSE_PROJECT_NAME": "isolated-validation",
        "no_proxy": lower,
        "NO_PROXY": upper,
    }
    result = subprocess.run(
        [
            "bash",
            str(sandbox_runner),
            "bash",
            "-c",
            'printf "%s\\n%s\\n%s\\n" "$no_proxy" "$NO_PROXY" "$COMPOSE_PROJECT_NAME"',
        ],
        cwd=tmp_path,
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
