"""Contract guards for host-side Makefile proxy handling."""

import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
MAKEFILE = REPO_ROOT / "Makefile"
LOCALHOSTS = "127.0.0.1,localhost"
LOWER_BYPASS = f'no_proxy="{LOCALHOSTS}${{no_proxy:+,${{no_proxy}}}}"'
UPPER_BYPASS = f'NO_PROXY="{LOCALHOSTS}${{NO_PROXY:+,${{NO_PROXY}}}}"'

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


def test_localhost_proxy_bypass_has_one_makefile_definition() -> None:
    text = MAKEFILE.read_text(encoding="utf-8")
    assert text.count(LOCALHOSTS) == 1, (
        "localhost exclusions must stay in the shared Makefile definition, not recipes"
    )


@pytest.mark.parametrize("target", HOST_SIDE_SERVICE_TARGETS)
def test_host_side_service_targets_render_shared_proxy_bypass(target: str) -> None:
    recipe = _render_target(target)
    assert LOWER_BYPASS in recipe, f"{target} does not set lowercase no_proxy"
    assert UPPER_BYPASS in recipe, f"{target} does not set uppercase NO_PROXY"


def test_offline_target_does_not_receive_local_service_environment() -> None:
    recipe = _render_target("lint")
    assert "no_proxy=" not in recipe
    assert "NO_PROXY=" not in recipe


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
def test_localhost_proxy_bypass_preserves_inherited_exclusions(
    lower: str,
    upper: str,
    expected_lower: str,
    expected_upper: str,
) -> None:
    probe = (
        "proxy-probe:\n"
        '\t@$(LOCALHOST_PROXY_BYPASS) sh -c \'printf "%s\\n%s\\n" '
        '"$$no_proxy" "$$NO_PROXY"\''
    )
    env = {**os.environ, "no_proxy": lower, "NO_PROXY": upper}
    result = subprocess.run(
        ["make", "--no-print-directory", "--eval", probe, "proxy-probe"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.stdout.splitlines() == [expected_lower, expected_upper]
