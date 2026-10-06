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

Maintained documentation includes, where applicable to the delivered scope:

- `README.md`;
- `ROADMAP.md`;
- `PROGRESS.md`;
- `docs/data-model.md`;
- `docs/data-contracts.md`;
- ADRs;
- runbooks;
- benchmark reports;
- dashboard screenshots.

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

Delivered operational failure modes should have runbooks where recovery is not
obvious.

Examples:

- failed API ingestion;
- Kafka lag;
- dbt test failure;
- ClickHouse outage;
- malformed supplier file.

### 41.3 Active task plan

The active task plan lives in `docs/plans/active.md` — exactly one file,
referenced from `PROGRESS.md` (Current focus) and picked up via AGENTS.md
§5.1. It is a session-resume artifact, not documentation:

- holds what an agent needs to continue mid-task: goal, files to read first,
  decisions already made, a step checklist, validation commands, scope fence;
- written in the same session that planned the task; updated when decisions
  change during implementation;
- replaced (or deleted) in the same commit as the work it describes —
  completed plans are not archived in the tree, git history keeps them.

Division of labor stays: `ROADMAP.md` owns acceptance criteria, ADRs own
architectural decisions, `PROGRESS.md` owns status, the active plan owns
execution detail of the single current task.

---

## 42. ADR policy

An ADR is required for significant decisions such as:

- reopening or expanding the Phase 8 scope frozen by ADR 0009;
- adding/removing a platform technology;
- replacing a catalog;
- changing source-of-truth ownership;
- changing orchestration strategy;
- introducing a new persistence pattern;
- changing CI platform;
- changing major serving architecture;
- introducing cluster orchestration such as Kubernetes (which also requires
  explicitly reopening the ADR 0009 scope).

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
- follow the maintenance scope and architecture rules in AGENTS.md and ADR 0009;
- use pinned/reproducible dependencies;
- do not hardcode secrets;
- add or update tests;
- add healthchecks/config validation where applicable;
- update documentation and .env.example if configuration changes;
- prefer idempotent operations;
- do not introduce a new feature domain or platform technology without an ADR
  that explicitly reopens the scope.

Before finishing:
- run relevant lint/tests/smoke checks;
- report changed files;
- report commands actually executed and their result;
- report known limitations;
- identify the next logical maintenance action, if any, without inventing a
  new feature phase.
```

---

## 52. Portfolio completion scenario

The completed Phase 8 capstone should reproducibly demonstrate, within the
limitations disclosed in `PROGRESS.md`:

1. A deterministic PostgreSQL baseline exists.
2. Supported batch sources preserve raw payloads and load Iceberg Bronze.
3. A customer, order, or payment is created, changed, or deleted in PostgreSQL.
4. Debezium captures the change from PostgreSQL WAL and Kafka transports it.
5. The consumer preserves the raw/immutable CDC representation in Bronze before
   committing the corresponding offset.
6. Airflow freezes a healthy, stable Kafka boundary.
7. dbt produces typed/delete-aware Silver state, Gold facts/dimensions, and
   business marts; critical tests pass.
8. The serving publication atomically updates rebuildable ClickHouse tables.
9. Superset reflects the KPI state through a read-only connection.
10. Re-running the same logical work does not create duplicate business data.
11. Recovery procedures for delivered failure modes are documented and
    demonstrable.

The project is not considered complete merely because all containers start,
and its production-like educational status must not be presented as production
readiness.
