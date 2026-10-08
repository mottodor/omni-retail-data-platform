# Operational runbooks

Use these procedures for delivered failure and recovery scenarios. Start with
the narrowest matching runbook; do not use destructive reset commands as a
generic troubleshooting step.

| Scenario | Runbook | Covers |
| --- | --- | --- |
| PostgreSQL CDC, connector/task failure, consumer lag, stale WAL, replay, or transport reset | [Kafka and CDC](kafka-cdc.md) | First bootstrap, health and lag inspection, offset-after-Iceberg semantics, stable-boundary refresh, and explicit recovery paths |
| Rebuild Bronze from preserved raw archive objects | [Bronze rebuild](bronze-rebuild.md) | Scope checks, destructive confirmation, rebuild, validation, and recovery from a failed rebuild |
| Malformed or rejected supplier input | [Bad supplier file](bad-supplier-file.md) | Quarantine inspection, reason classification, corrected-file replay, and audit checks |
| ClickHouse outage or serving mismatch | [ClickHouse outage](clickhouse-outage.md) | Failure behavior, rebuild from Iceberg Gold, atomic publication, and reconciliation |
| Superset bootstrap, connectivity, assets, or dashboard recovery | [Superset](superset.md) | Health checks, database connectivity, sanitized asset import, and dashboard recovery |

## Before recovery

1. Read the matching runbook completely.
2. Preserve logs and the identifiers relevant to the failure: run ID, batch ID,
   source object, Kafka topic/partition/offset, or mart name.
3. Confirm which Compose profiles are running with `docker compose ps`.
4. Treat Iceberg as the analytical source of truth and ClickHouse as a derived
   serving copy.
5. Back up local state when the runbook requires it.
6. Run only commands whose destructive scope is understood and explicitly
   documented.

General startup, profile, and validation commands are in the
[README](../../README.md#local-setup). The guided source-to-BI route is in the
[learning path](../learning-path.md#full-demo). Known environment limitations
and open technical debt are recorded in [PROGRESS.md](../../PROGRESS.md).
