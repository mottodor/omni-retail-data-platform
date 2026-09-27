-- Service accounts: least privilege (AGENTS §10, §40; ADR 0004).
--   omni_publisher  — every table-level grant on analytics.* (the
--                     truncate/insert/EXCHANGE publish cycle); no access
--                     management, nothing outside analytics;
--   superset_reader — SELECT only (BI / Superset, Phase 7).
-- Passwords arrive as clickhouse-client query parameters from the
-- clickhouse-init environment, never as literals in this file. Rotating a
-- password later is a manual `ALTER USER ... IDENTIFIED BY '<new>'`
-- (documented in the Phase 6 slice 3 runbook); re-running this migration
-- is a no-op once the ledger has recorded it.

CREATE USER IF NOT EXISTS omni_publisher IDENTIFIED BY {publisher_password:String};
GRANT ALL ON analytics.* TO omni_publisher;

CREATE USER IF NOT EXISTS superset_reader IDENTIFIED BY {reader_password:String};
GRANT SELECT ON analytics.* TO superset_reader;
