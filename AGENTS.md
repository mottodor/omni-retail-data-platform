# AGENTS.md — OmniRetail Data Platform

This file defines the operating rules for AI coding agents working on **OmniRetail Data Platform**.

It is the **core** of a two-layer rule set:

- this file — always loaded; contains the invariants that apply to every task;
- `docs/agent/*.md` — topical guides, read on demand per the routing table below.

The repository is a production-like Data Engineering portfolio project. The goal is not to maximize the number of technologies used. The goal is to build a reproducible, testable, observable, and explainable data platform that demonstrates commercial-grade engineering practices.

---

## 1. Authority and source of truth

Before changing code, always read:

1. `AGENTS.md` (this file) — mandatory engineering and agent rules.
2. The topical guide(s) under `docs/agent/` that match the task — routing table below.
3. `ROADMAP.md` — implementation order, scope, acceptance criteria, and target architecture.
4. Relevant files under `docs/adr/` — architectural decisions already made.
5. Relevant tests and current implementation.

When instructions conflict, use this precedence:

1. Explicit user request for the current task.
2. `AGENTS.md` — this core file together with the topical guides under `docs/agent/`; on any internal conflict, the core file wins.
3. Accepted ADRs.
4. `ROADMAP.md`.
5. Existing implementation conventions.

Do not silently override an accepted architectural decision.

If an architectural change is required, create or update an ADR before implementing the change.

---

## Topical guides (docs/agent/)

Detailed component rules live under `docs/agent/` and are **normative**: before working in one of these areas, read the matching guide. Each rule has a single home — this core file does not duplicate guide content.

| Task involves | Read first |
|---|---|
| Ingestion (batch, REST API, files/S3), source generator, PostgreSQL source schema, pipeline logging | `docs/agent/ingestion.md` |
| dbt models, analytical modeling, marts, SQL style | `docs/agent/dbt-modeling.md` |
| Iceberg, Polaris, Trino | `docs/agent/lakehouse.md` |
| Airflow DAGs and orchestration | `docs/agent/airflow.md` |
| Kafka, Debezium, CDC (Phase E) | `docs/agent/streaming.md` |
| ClickHouse, Superset, Spark (serving, Phase F) | `docs/agent/serving-bi.md` |
| Data quality, data contracts, backfills, late-arriving data, failure engineering | `docs/agent/reliability.md` |
| Docker Compose, Makefile, Git/GitHub workflow, GitHub Actions, observability, lineage | `docs/agent/platform-ops.md` |
| Testing strategy, test data, performance engineering | `docs/agent/testing-perf.md` |
| Security baseline, documentation rules, ADR policy, migrations, task prompt template | `docs/agent/engineering-practices.md` |

Moved sections map — original section numbers are preserved inside the guides, so references of the form "AGENTS §N" resolve via this table:

- §9, §14–17, §46 → `docs/agent/ingestion.md`
- §18 → `docs/agent/streaming.md`
- §19 → `docs/agent/airflow.md`
- §20–21, §44 → `docs/agent/dbt-modeling.md`
- §22–24 → `docs/agent/lakehouse.md`
- §25–27 → `docs/agent/serving-bi.md`
- §28–32 → `docs/agent/reliability.md`
- §12–13, §33–36 → `docs/agent/platform-ops.md`
- §37–39 → `docs/agent/testing-perf.md`
- §40–42, §45, §50, §52 → `docs/agent/engineering-practices.md`

Sections that remain in this core file: §1–8, §10–11, §43, §47–49, §51, §53.

---

## 2. Project objective

OmniRetail is a production-like e-commerce data platform intended to demonstrate practical Data Engineering skills:

- batch ingestion;
- REST API ingestion;
- file/S3 ingestion;
- PostgreSQL OLTP extraction;
- CDC;
- event streaming;
- lakehouse architecture;
- Iceberg table management;
- SQL transformation with dbt;
- analytical modeling;
- ClickHouse serving;
- BI with Apache Superset;
- orchestration with Apache Airflow;
- distributed processing with Spark where justified;
- data quality;
- observability;
- lineage;
- CI/CD;
- failure recovery;
- idempotency and backfills;
- performance engineering.

The project must remain suitable for a local workstation:

- Windows 11;
- WSL2;
- 32 GB physical RAM;
- Docker Compose as the primary runtime.

---

## 3. Fixed architecture decisions

The following decisions are already accepted and MUST NOT be changed without an ADR.

### 3.1 Core stack

- Python 3.12 baseline.
- Apache Airflow 2.11.2 baseline.
- dbt Core 1.10.x baseline; dbt-trino adapter 1.9.x; exact versions locked in `uv.lock`.
- Trino 476+; initial pinned target is Trino 483.
- Apache Iceberg.
- Apache Polaris as Iceberg REST catalog.
- PostgreSQL 16+.
- ClickHouse.
- Apache Superset.
- MinIO as local S3-compatible object storage.
- Apache Kafka.
- Debezium.
- Apache Spark.
- Prometheus.
- Grafana.
- OpenLineage.
- Marquez.
- Docker Compose.
- GitHub.
- GitHub Actions.
- GHCR.

Later exercises:

- Airflow 2 -> Airflow 3 migration;
- GitHub Actions -> GitLab CI migration;
- Kubernetes only after the Docker Compose platform is complete.

### 3.2 Storage and serving responsibilities

**Iceberg is the source of truth.**

**ClickHouse is a derived serving layer.**

ClickHouse must be rebuildable from Iceberg Gold datasets.

Do not introduce business logic that exists only inside ClickHouse unless it is explicitly serving-specific and documented.

### 3.3 BI responsibilities

Primary BI path:

```text
Superset -> ClickHouse
```

Ad-hoc exploration path:

```text
Superset -> Trino -> Iceberg
```

Grafana is for platform observability, not business BI.

### 3.4 Processing responsibilities

Use:

- dbt + Trino for relational analytical transformations;
- Spark only when the workload justifies distributed processing;
- Airflow for orchestration, not for storing transformation SQL;
- Kafka for transport/event streaming;
- Debezium for PostgreSQL CDC;
- MinIO for object storage;
- Polaris for Iceberg catalog metadata.

---

## 4. Implementation order is mandatory

Do not build the final architecture all at once.

The project follows a **vertical-slice-first** strategy.

The first working end-to-end slice is:

```text
PostgreSQL
    ->
batch extraction
    ->
MinIO
    ->
Iceberg Bronze
    ->
Trino + dbt
    ->
Gold mart
    ->
ClickHouse
    ->
Superset
```

Do not introduce Kafka, Debezium, Spark, Marquez, or full observability before the first slice works end-to-end unless the current GitHub issue explicitly belongs to those later phases.

The initial issue sequence is expected to follow the backlog in `ROADMAP.md`.

---

## 5. Agent workflow

For every task, follow this sequence.

### 5.1 Before coding

1. Read the GitHub issue or task specification.
2. Read `AGENTS.md` and the topical guide(s) under `docs/agent/` that match the task (routing table above).
3. Read `PROGRESS.md` — current project state: phase/slice status, current focus, deferred items. If it references an active task plan (`docs/plans/active.md`), read it and resume from its checklist instead of re-deriving the plan.
4. Read the relevant section of `ROADMAP.md`.
5. Read relevant ADRs.
6. Inspect existing implementation.
7. Inspect existing tests.
8. Identify the smallest correct scope.
9. Identify affected services, contracts, schemas, and documentation.
10. Check whether the task requires a new dependency or architectural decision.
11. If a new technology or architecture decision is required, create an ADR first.
12. When checking upstream documentation for any technology (Trino, dbt, Airflow, Iceberg, ClickHouse, Superset, Debezium, Spark, etc.), use the `context7` tool.

Do not start by rewriting unrelated files.

### 5.2 During implementation

- Keep the change limited to the issue.
- Preserve existing behavior unless the issue explicitly changes it.
- Prefer small, reversible changes.
- Add tests with the implementation.
- Use explicit configuration.
- Keep services reproducible.
- Keep operations idempotent where applicable.
- Add useful structured logging.
- Update documentation when behavior changes.

### 5.3 Before finishing

Run all relevant checks that are reasonably available in the current environment.

At minimum consider:

```bash
make lint
make test
```

Then run component-specific validation where applicable:

```bash
docker compose config
dbt parse
dbt compile
dbt build
pytest airflow/tests
make smoke-core
make integration
```

Do not claim a check passed unless it was actually executed successfully.

If a check cannot be executed, clearly state why.

---

## 6. Definition of Done

A task is not complete merely because the code exists.

A change is complete when all applicable conditions are satisfied:

- implementation matches the issue;
- relevant acceptance criteria from `ROADMAP.md` are satisfied;
- tests exist for critical behavior;
- existing relevant tests pass;
- linting passes;
- configuration is reproducible;
- secrets are not committed;
- dependencies are pinned or constrained appropriately;
- new services have healthchecks where practical;
- startup order does not depend on arbitrary sleeps when a healthcheck can be used;
- operations are idempotent where required;
- retries do not create duplicate business data;
- documentation is updated;
- `PROGRESS.md` is updated when the change affects phase/slice status, current focus, or deferred items;
- `.env.example` is updated when configuration changes;
- logs expose useful execution context;
- failure behavior is defined;
- manual verification steps are documented;
- no unrelated refactoring was included.

---

## 7. Repository structure

Follow the repository structure defined by `ROADMAP.md`.

Expected top-level layout (src-layout: Python code lives in the `src/omni_retail` package):

```text
.github/
airflow/
dbt/
infrastructure/
postgres/
src/
trino/
tests/
docs/
docker-compose.yml
Makefile
pyproject.toml
uv.lock
.env.example
AGENTS.md
PROGRESS.md
ROADMAP.md
README.md
```

Python code lives under `src/omni_retail/` (`ingestion/`, `generators/`, `lakehouse/`); custom Dockerfiles and init scripts live under `infrastructure/` (`airflow/`, `mock_api/`, `scripts/`).

Agent topical guides live under `docs/agent/` (see routing table above).

Directories for later phases (`clickhouse/`, `superset/`, `kafka/`, `spark/`, `observability/`) are created only when their roadmap phase is reached, not in advance.

Do not create alternative top-level directories for existing concerns without a clear reason.

Prefer grouping configuration with its owning component.

---

## 8. Coding principles

### 8.1 General

Prefer:

- explicit code over magic;
- simple code over premature abstraction;
- composition over inheritance;
- deterministic behavior;
- typed interfaces;
- pure functions for transformation logic;
- dependency injection for external systems where useful;
- small modules;
- reusable domain-level utilities only after repetition becomes clear.

Avoid:

- giant utility modules;
- hidden global state;
- import-time network calls;
- import-time database connections;
- hardcoded environment-specific values;
- copy/paste pipelines that should share a tested primitive;
- abstractions created only to reduce line count.

### 8.2 Python style

Use Python 3.12-compatible syntax unless an ADR changes the baseline.

Use type hints for:

- public functions;
- service boundaries;
- domain models;
- configuration objects;
- API clients;
- ingestion functions;
- reusable utilities.

Prefer `pathlib.Path` over ad-hoc path string manipulation.

Prefer structured models for configuration/data contracts where they improve correctness.

Do not log secrets or credentials.

### 8.3 Exceptions

Do not catch broad exceptions without a reason.

Bad:

```python
try:
    ...
except Exception:
    pass
```

Acceptable only when:

- the failure is intentionally isolated;
- the exception is logged with context;
- the resulting behavior is explicit;
- retry/quarantine semantics are documented.

Preserve the original exception chain when re-raising:

```python
raise IngestionError(...) from exc
```

---

## 10. Configuration and secrets

All runtime configuration must be externalized.

Use:

- environment variables;
- configuration files committed without secrets;
- `.env.example`;
- GitHub Actions secrets for CI-only credentials where necessary.

Never commit:

- `.env`;
- passwords;
- API tokens;
- access keys;
- private keys;
- GitHub tokens;
- MinIO credentials;
- PostgreSQL passwords;
- ClickHouse passwords;
- Superset secrets.

Use separate service accounts whenever practical.

Apply least privilege.

Examples:

- Superset uses `superset_reader`;
- BI users do not receive ClickHouse write permissions;
- application ingestion users receive only required source permissions.

---

## 11. Dependency policy

Do not use floating dependency versions for reproducible infrastructure.

Avoid Docker image tags such as:

```text
latest
stable
edge
```

Pin meaningful versions.

For Python dependencies, use **uv** as the package and dependency manager. The lockfile (`uv.lock`) is committed and is the source of truth for reproducible installs; `pyproject.toml` declares the project metadata and dependencies. All Python environments (local, CI, containers) are created with `uv sync` — do not use `pip install` or a different package manager.

Do not introduce a new dependency if the standard library or an existing dependency solves the task cleanly.

Every new major dependency must have:

- a clear use case;
- acceptable maintenance status;
- compatibility with Python/runtime constraints;
- minimal resource impact;
- documented reason.

A new platform service requires an ADR.

---

## 43. Schema and naming conventions

Prefer predictable lower-case snake_case names.

### 43.1 SQL objects

Use names such as:

```text
bronze.orders
silver.orders
gold.fact_orders
analytics.mart_daily_sales
```

### 43.2 Python

Use:

- modules: `snake_case`;
- functions: `snake_case`;
- classes: `PascalCase`;
- constants: `UPPER_SNAKE_CASE`.

### 43.3 Identifiers

Prefer explicit names:

```text
customer_id
order_id
batch_id
run_id
source_updated_at
ingested_at
```

Avoid ambiguous names such as:

```text
id
date
data
value
tmp
```

when domain-specific naming is possible.

---

## 47. Scope control

Do not expand a GitHub issue because an adjacent improvement "would be nice".

If you notice unrelated work:

1. document it in the completion summary;
2. propose a follow-up issue;
3. do not implement it unless required for correctness.

This rule is especially important for AI-generated refactors.

---

## 48. Prohibited patterns

Agents MUST NOT:

- rewrite the repository without need;
- add technologies for novelty;
- use Spark for tiny workloads without justification;
- place dbt business SQL inside Airflow DAGs;
- use ClickHouse as the only persistent source of business truth;
- expose ClickHouse publicly just for Superset;
- store secrets in Git;
- use `latest` image tags in pinned infrastructure;
- disable tests to make CI pass;
- silently swallow errors;
- create retry loops without termination;
- use arbitrary `sleep` as service readiness when a healthcheck is possible;
- make destructive maintenance the default;
- add public APIs to unit tests;
- create manual-only infrastructure steps without documenting them;
- introduce a service without a reason/ADR;
- claim exactly-once processing without end-to-end evidence;
- claim a test was executed when it was not;
- bypass roadmap ordering without an explicit task requirement.

---

## 49. Expected agent completion report

At the end of every task, report concisely:

If the task changes phase/slice status, the current focus, or the deferred list, update `PROGRESS.md` in the same change.

### Implemented

List meaningful changes.

### Changed files

List files created/modified.

### Validation

List commands actually executed and whether they passed.

Example:

```text
make lint             PASS
pytest tests/unit     PASS
docker compose config PASS
make smoke-core       PASS
```

### Manual verification

Give exact commands or UI steps if manual verification is still useful.

### Known limitations

State remaining limitations honestly.

### Follow-up

Recommend the next logical GitHub issue, but do not implement it unless requested. Record lasting follow-ups in `PROGRESS.md` (Deferred / follow-ups).

---

## 51. Phase gates

The agent must respect the following gates.

### Gate A — Repository foundation

Before data-platform implementation:

- Python tooling works;
- `make lint` works;
- `make test` works;
- GitHub Actions basic CI exists;
- `.env.example` exists;
- ADR mechanism exists.

### Gate B — Core lakehouse

Before orchestration:

- PostgreSQL runs;
- MinIO runs;
- Polaris runs;
- Trino runs;
- Trino can create/read an Iceberg table in MinIO;
- persistence survives restart;
- smoke test exists.

### Gate C — First vertical slice

Before Kafka/Spark:

- source orders exist;
- batch extraction exists;
- Bronze exists;
- dbt Silver exists;
- Gold `fact_orders` exists;
- `mart_daily_sales` exists;
- ClickHouse publication exists;
- Superset Revenue dashboard works.

### Gate D — Orchestration

Before advanced streaming:

- Airflow DAGs parse;
- batch ingestion is orchestrated;
- retries/backfills are safe;
- independent pipelines fail independently.

### Gate E — CDC

Before calling CDC complete:

- Debezium captures source changes;
- Kafka transports events;
- create/update/delete work;
- consumer restart recovers;
- duplicate events do not corrupt final state.

### Gate F — Spark

Before calling Spark complete:

- workload justifies Spark;
- deterministic test exists;
- sessionization or equivalent distributed logic works;
- resource configuration fits the local workstation.

### Gate G — Production-like operation

Before declaring the project portfolio-ready:

- data quality exists;
- failure scenarios exist;
- backfill works;
- Iceberg maintenance works;
- monitoring exists;
- lineage exists;
- CI integration tests exist;
- ClickHouse is rebuildable from Iceberg;
- documentation and runbooks exist.

---

## 53. Final rule

Every technology must solve a concrete engineering problem.

When choosing between:

- a simpler implementation that is correct, testable, reproducible, and aligned with the architecture;
- a more complex implementation that merely demonstrates another tool;

choose the simpler implementation.

Complexity should be introduced only when the roadmap creates a real requirement for it.
