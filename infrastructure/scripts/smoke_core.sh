#!/usr/bin/env bash
# End-to-end smoke test for the core lakehouse (Gate B):
# Trino -> Polaris REST catalog -> Iceberg table -> MinIO object storage.
# Appends one row per run to iceberg.demo.healthcheck and requires the
# accumulated row count to be positive.
set -euo pipefail

cd "$(dirname "$0")/../.."

run_id="smoke-$(date -u +%Y%m%dT%H%M%SZ)"

trino_exec() {
    docker compose exec -T trino trino --output-format=CSV --execute "$1"
}

trino_exec "CREATE SCHEMA IF NOT EXISTS iceberg.demo" >/dev/null
trino_exec "CREATE TABLE IF NOT EXISTS iceberg.demo.healthcheck (run_id varchar, checked_at timestamp)" >/dev/null
trino_exec "INSERT INTO iceberg.demo.healthcheck VALUES ('$run_id', now())" >/dev/null

row_count="$(trino_exec "SELECT count(*) FROM iceberg.demo.healthcheck" | tail -n 1 | tr -d '"')"

if ! [[ "$row_count" =~ ^[0-9]+$ ]] || ((row_count < 1)); then
    echo "smoke-core: FAILED run_id=$run_id row_count=$row_count" >&2
    exit 1
fi

echo "smoke-core: PASS run_id=$run_id row_count=$row_count"
