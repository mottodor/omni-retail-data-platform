#!/usr/bin/env bash
# Explicit, bounded recovery for a full archive -> Bronze rebuild.
set -euo pipefail

cd "$(dirname "$0")/../.."

if [[ ! -f .env ]]; then
    echo "bronze-rebuild: .env is required (copy .env.example first)" >&2
    exit 2
fi
# Preserve explicit one-run overrides while loading credentials/defaults from .env.
caller_schema="${ICEBERG_BRONZE_SCHEMA:-}"
caller_max_polaris_restarts="${BRONZE_REBUILD_MAX_POLARIS_RESTARTS:-}"
caller_max_trino_restarts="${BRONZE_REBUILD_MAX_TRINO_RESTARTS:-}"
caller_health_timeout_seconds="${BRONZE_REBUILD_HEALTH_TIMEOUT_SECONDS:-}"
set -a
# shellcheck disable=SC1091
source .env
set +a

schema="${caller_schema:-${ICEBERG_BRONZE_SCHEMA:-bronze}}"
if ! [[ "$schema" =~ ^[a-z][a-z0-9_]{0,127}$ ]] || {
    [[ "$schema" != "bronze" ]] && [[ "$schema" != *_bronze ]]
}; then
    echo "bronze-rebuild: refusing unsafe Bronze schema name: $schema" >&2
    exit 2
fi

max_polaris_restarts="${caller_max_polaris_restarts:-${BRONZE_REBUILD_MAX_POLARIS_RESTARTS:-2}}"
max_trino_restarts="${caller_max_trino_restarts:-${BRONZE_REBUILD_MAX_TRINO_RESTARTS:-2}}"
health_timeout_seconds="${caller_health_timeout_seconds:-${BRONZE_REBUILD_HEALTH_TIMEOUT_SECONDS:-120}}"
if ! [[ "$max_polaris_restarts" =~ ^[0-9]+$ ]] \
    || ! [[ "$max_trino_restarts" =~ ^[0-9]+$ ]] \
    || ! [[ "$health_timeout_seconds" =~ ^[1-9][0-9]*$ ]]; then
    echo "bronze-rebuild: restart budgets must be >= 0 and health timeout must be positive" >&2
    exit 2
fi

trino_exec() {
    docker compose --profile core exec -T trino trino --execute "$1"
}

wait_for_polaris() {
    local deadline
    deadline=$(( $(date +%s) + health_timeout_seconds ))
    until docker compose --profile core exec -T polaris curl --fail --silent \
        http://localhost:8182/q/health >/dev/null; do
        if (( $(date +%s) >= deadline )); then
            echo "bronze-rebuild: Polaris did not become healthy within ${health_timeout_seconds}s" >&2
            return 1
        fi
        sleep 2
    done
}

wait_for_trino() {
    local deadline
    deadline=$(( $(date +%s) + health_timeout_seconds ))
    until docker compose --profile core exec -T trino trino --execute "select 1" \
        >/dev/null 2>&1; do
        if (( $(date +%s) >= deadline )); then
            echo "bronze-rebuild: Trino did not become healthy within ${health_timeout_seconds}s" >&2
            return 1
        fi
        sleep 2
    done
}

run_loader() {
    no_proxy="127.0.0.1,localhost,${no_proxy:-}" \
        NO_PROXY="127.0.0.1,localhost,${NO_PROXY:-}" \
        uv run python -m omni_retail.lakehouse.bronze run-new
}

echo "bronze-rebuild: clearing only iceberg.${schema}; archive, source, Silver, Gold, and serving are untouched"
trino_exec "drop schema if exists iceberg.${schema} cascade" >/dev/null

polaris_restart_attempt=0
trino_restart_attempt=0
run_attempt=0
while true; do
    run_attempt=$((run_attempt + 1))
    echo "bronze-rebuild: run-new attempt=${run_attempt} polaris_restarts=${polaris_restart_attempt} trino_restarts=${trino_restart_attempt}"
    set +e
    run_loader
    status=$?
    set -e
    if (( status == 0 )); then
        echo "bronze-rebuild: complete schema=${schema} polaris_restarts=${polaris_restart_attempt} trino_restarts=${trino_restart_attempt}"
        exit 0
    fi
    # Exit 75 is the Trino 483/Polaris catalog failure; 76 is a failed Trino
    # HTTP connection (including an OOM-triggered container restart). All other
    # failures are data, schema, manifest, or unsupported infrastructure errors.
    case "$status" in
        75)
            if (( polaris_restart_attempt >= max_polaris_restarts )); then
                echo "bronze-rebuild: Polaris retry budget exhausted restarts=${polaris_restart_attempt}" >&2
                exit "$status"
            fi
            polaris_restart_attempt=$((polaris_restart_attempt + 1))
            echo "bronze-rebuild: transient catalog failure; restarting Polaris attempt=${polaris_restart_attempt}/${max_polaris_restarts}"
            docker compose --profile core restart polaris
            wait_for_polaris
            echo "bronze-rebuild: Polaris healthy; resuming run-new without clearing Bronze again"
            ;;
        76)
            if (( trino_restart_attempt >= max_trino_restarts )); then
                echo "bronze-rebuild: Trino retry budget exhausted restarts=${trino_restart_attempt}" >&2
                exit "$status"
            fi
            trino_restart_attempt=$((trino_restart_attempt + 1))
            echo "bronze-rebuild: transient Trino failure; restarting Trino attempt=${trino_restart_attempt}/${max_trino_restarts}"
            docker compose --profile core restart trino
            wait_for_trino
            echo "bronze-rebuild: Trino healthy; resuming run-new without clearing Bronze again"
            ;;
        *)
            echo "bronze-rebuild: loader failed with non-transient exit=${status}; no restart attempted" >&2
            exit "$status"
            ;;
    esac
done
