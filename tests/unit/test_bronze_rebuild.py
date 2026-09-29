"""Static safety guards for the explicit Bronze rebuild wrapper."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "infrastructure" / "scripts" / "bronze_rebuild.sh"


def test_bronze_rebuild_is_bounded_and_targets_only_bronze_schema() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "set -euo pipefail" in text
    assert "drop schema if exists iceberg.${schema} cascade" in text
    assert "BRONZE_REBUILD_MAX_POLARIS_RESTARTS" in text
    assert "caller_max_trino_restarts" in text
    assert "BRONZE_REBUILD_MAX_TRINO_RESTARTS" in text
    assert "BRONZE_REBUILD_HEALTH_TIMEOUT_SECONDS" in text
    assert "docker compose --profile core restart polaris" in text
    assert "docker compose --profile core restart trino" in text
    assert "wait_for_trino" in text
    assert 'trino --execute "select 1"' in text
    assert 'case "$status" in' in text
    assert "without clearing Bronze again" in text
