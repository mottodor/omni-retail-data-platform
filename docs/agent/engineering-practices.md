# Agent guide — Engineering practices (security, docs, ADRs, migrations)

Part of the repository agent rules, split out of the monolithic `AGENTS.md`.

Read this guide before working on: security-relevant configuration,
documentation updates, ADRs, database/infrastructure migrations, or when
composing a task prompt for an AI agent.

Precedence: explicit user request > `AGENTS.md` core > accepted ADRs > this
guide > `ROADMAP.md` > existing implementation conventions. On conflict with
the `AGENTS.md` core, the core wins.

Contains original AGENTS.md sections 40–42, 45, 50, 52. Section numbers are
preserved so existing references of the form "AGENTS §N" keep resolving.
Core secret-handling rules remain in the `AGENTS.md` core (§10).

---

## 40. Security baseline

Required baseline:

- `.env` ignored;
- `.env.example` contains placeholders only;
- least-privilege service accounts;
- no production-like admin account used by BI;
- only required host ports exposed;
- internal databases remain Docker-network-local where possible;
- GitHub secrets used only for trusted CI needs;
- credentials never printed in CI logs;
- default passwords avoided for exposed interfaces.

Do not weaken security simply to make local connectivity easier.

---

## 41. Documentation rules

Code changes that affect usage, architecture, or operations require documentation updates.

Expected documentation eventually includes:

- `README.md`;
- `docs/architecture.md`;
- `docs/data-model.md`;
- `docs/data-contracts.md`;
- `docs/sla-slo.md`;
- ADRs;
- runbooks;
- benchmark reports;
- incident postmortem example.

### 41.1 README

Keep README portfolio-friendly.

It should emphasize:

- business problem;
- architecture;
- how to run;
- what is implemented;
- screenshots;
- benchmarks;
- reliability scenarios.

Do not turn README into an internal development dump.

### 41.2 Runbooks

Operational failures should eventually have runbooks.

Examples:

- failed API ingestion;
- Kafka lag;
- dbt test failure;
- ClickHouse outage;
- malformed supplier file.

---

## 42. ADR policy

An ADR is required for significant decisions such as:

- adding/removing a platform technology;
- replacing a catalog;
- changing source-of-truth ownership;
- changing orchestration strategy;
- introducing a new persistence pattern;
- changing CI platform;
- changing major serving architecture;
- introducing Kubernetes.

An ADR should contain:

```text
Title
Status
Context
Decision
Alternatives considered
Consequences
Rollback / migration considerations
```

Do not create ADRs for trivial implementation details.

---

## 45. Migration rules

Database and infrastructure changes should be migration-friendly.

For schema changes:

- create versioned migration;
- make repeated execution safe where practical;
- document downgrade/rollback when important;
- update fixtures/tests.

For ClickHouse and PostgreSQL, do not rely on manual console-only changes.

---

## 50. Recommended task prompt

A task sent to an AI coding agent can use this template:

```text
Implement GitHub issue <N> for OmniRetail Data Platform.

Before changing code:
1. Read AGENTS.md (core plus the matching docs/agent/ guide), ROADMAP.md, and relevant ADRs.
2. Inspect the existing implementation and tests.
3. Keep the scope limited to this issue.

Requirements:
- preserve existing behavior unless the issue explicitly changes it;
- follow the architecture and sequencing rules in AGENTS.md;
- use pinned/reproducible dependencies;
- do not hardcode secrets;
- add or update tests;
- add healthchecks/config validation where applicable;
- update documentation and .env.example if configuration changes;
- prefer idempotent operations;
- do not introduce a new platform technology without an ADR.

Before finishing:
- run relevant lint/tests/smoke checks;
- report changed files;
- report commands actually executed and their result;
- report known limitations;
- identify the next logical issue without implementing it.
```

---

## 52. Portfolio completion scenario

The final platform should reproducibly demonstrate:

1. An order is created or changed in PostgreSQL.
2. Debezium captures the change from PostgreSQL WAL.
3. The change enters Kafka.
4. Bronze preserves raw/immutable representation.
5. Silver produces cleaned/current analytical state.
6. dbt updates facts/dimensions.
7. Gold updates business marts.
8. The serving publication updates ClickHouse.
9. Superset reflects the KPI change.
10. Airflow shows orchestration state.
11. Grafana shows platform health/freshness.
12. Marquez shows lineage.
13. Re-running the same logical work does not create duplicates.
14. Historical backfill is reproducible.
15. Failure recovery is documented and demonstrable.

The project is not considered complete merely because all containers start.
