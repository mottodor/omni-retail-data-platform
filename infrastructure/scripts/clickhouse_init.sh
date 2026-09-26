#!/bin/bash
# One-shot ClickHouse initializer (mirrors minio-init / polaris-init).
#
# Applies clickhouse/migrations/*.sql in lexical order and records each
# applied file (name + sha256) in the analytics.schema_migrations ledger
# table (AGENTS.md §45 — no console-only schema changes). Re-runs skip
# already-applied files; a mutated file that was applied earlier fails
# loudly instead of being silently skipped.
#
# Connection settings and the admin password reach clickhouse-client via
# its CLICKHOUSE_* environment variables (never as process arguments).
# Migration 0002 binds user passwords as query parameters, so no secret
# appears in SQL text either.
set -euo pipefail

host="${CLICKHOUSE_INIT_HOST:?CLICKHOUSE_INIT_HOST is required}"
port="${CLICKHOUSE_INIT_PORT:?CLICKHOUSE_INIT_PORT is required}"
admin_password="${CLICKHOUSE_ADMIN_PASSWORD:?CLICKHOUSE_ADMIN_PASSWORD is required}"
publisher_password="${CLICKHOUSE_PUBLISHER_PASSWORD:?CLICKHOUSE_PUBLISHER_PASSWORD is required}"
reader_password="${CLICKHOUSE_READER_PASSWORD:?CLICKHOUSE_READER_PASSWORD is required}"
migrations_dir="${CLICKHOUSE_MIGRATIONS_DIR:-/migrations}"

export CLICKHOUSE_HOST="$host"
export CLICKHOUSE_PORT="$port"
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD="$admin_password"

# Ledger pre-step: analytics and the ledger itself must exist before any
# migration runs (0001 re-asserts the database idempotently).
clickhouse-client --query "create database if not exists analytics"
clickhouse-client --query \
    "create table if not exists analytics.schema_migrations \
     (filename String, checksum String, applied_at DateTime DEFAULT now()) \
     ENGINE = MergeTree ORDER BY filename"

applied=0
skipped=0
for file in $(ls "$migrations_dir"/*.sql | sort); do
    name=$(basename "$file")
    checksum=$(sha256sum "$file" | cut -d' ' -f1)
    recorded=$(clickhouse-client --query \
        "select checksum from analytics.schema_migrations where filename = '$name' limit 1")
    if [ -n "$recorded" ]; then
        if [ "$recorded" != "$checksum" ]; then
            echo "clickhouse_init: ERROR $name changed after being applied (ledger checksum mismatch)" >&2
            echo "clickhouse_init: restore the file or add a new migration instead" >&2
            exit 1
        fi
        echo "clickhouse_init: skip $name (already applied)"
        skipped=$((skipped + 1))
        continue
    fi
    echo "clickhouse_init: applying $name"
    clickhouse-client --multiquery \
        --param_publisher_password="$publisher_password" \
        --param_reader_password="$reader_password" \
        < "$file"
    clickhouse-client --query \
        "insert into analytics.schema_migrations (filename, checksum) values ('$name', '$checksum')"
    applied=$((applied + 1))
done
echo "clickhouse_init: done (applied=$applied skipped=$skipped)"
