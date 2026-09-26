-- Serving database (ADR 0004). Iceberg stays the source of truth; the
-- analytics database in ClickHouse holds derived, rebuildable serving
-- copies of the Gold marts. clickhouse-init re-asserts this database
-- idempotently before creating its schema_migrations ledger.

CREATE DATABASE IF NOT EXISTS analytics;
