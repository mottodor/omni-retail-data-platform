# Agent guide — Platform operations (Docker, Make, Git, CI, diagnostics)

Part of the repository agent rules, split out of the monolithic `AGENTS.md`.

Read this guide before working on: `docker-compose.yml`, service profiles and
healthchecks, the `Makefile`, Git branches/commits/PRs, GitHub Actions
workflows, or runtime diagnostics for delivered services.

Precedence: explicit user request > `AGENTS.md` core > accepted ADRs > this
guide > `ROADMAP.md` > existing implementation conventions. On conflict with
the `AGENTS.md` core, the core wins.

Contains original AGENTS.md sections 12–13 and 33–36. Section numbers are
preserved so existing references of the form "AGENTS §N" keep resolving.

---

## 12. Docker and Docker Compose rules

Docker Compose is the primary local orchestration mechanism.

### 12.1 Compose profiles

Preserve the implemented logical profiles:

- `core`;
- `orchestration`;
- `streaming`;
- `bi`.

A new profile for an excluded subsystem requires an ADR that explicitly
supersedes ADR 0009.

Do not make all services start by default if they are not required.

The local workstation has limited RAM.

### 12.2 Resource awareness

Target environment:

```text
Windows 11
WSL2 memory: approximately 24 GB
WSL2 processors: approximately 6
WSL2 swap: approximately 8 GB
Physical RAM: 32 GB
```

Do not allocate excessive heap/memory defaults.

Kafka, Trino, Airflow, ClickHouse, and Superset do not need to run
simultaneously for every maintenance task.

### 12.3 Healthchecks

Add healthchecks for infrastructure services when practical.

Use dependency health conditions instead of fixed sleeps.

Bad:

```bash
sleep 30
```

Preferred:

- service healthcheck;
- retry loop with timeout;
- explicit readiness check.

### 12.4 Volumes

Persistent state must be intentional.

Document which volumes contain:

- PostgreSQL data;
- MinIO objects;
- ClickHouse data;
- Airflow metadata;
- Kafka state if persistent;
- Superset metadata.

A reset command may destroy local state only when clearly named and documented.

---

## 13. Makefile conventions

Prefer stable entry points for developer actions.

Stable developer entry points include:

```bash
make setup
make lint
make test
make unit
make integration
make up
make down
make logs
make reset
make smoke-core
make dbt-build
make airflow-test
```

Commands should be:

- discoverable;
- documented;
- composable;
- safe by default.

Do not hide destructive operations under harmless names.

---

## 33. Runtime diagnostics

A dedicated Prometheus/Grafana subsystem is not implemented and is outside the
final scope. Do not claim platform-wide monitoring or add a monitoring service
through routine maintenance.

Delivered diagnostics rely on:

- Docker healthchecks and service status;
- structured pipeline/task logs;
- Airflow run and task state;
- Kafka consumer-group lag and connector/task status;
- dbt tests and artifacts;
- smoke/integration checks;
- documented recovery procedures.

Maintenance changes must preserve enough context to identify what failed,
which dataset or service is affected, and how to recover. Adding a dedicated
monitoring subsystem requires an ADR that explicitly supersedes ADR 0009.

---

## 34. Lineage documentation

A dedicated OpenLineage/Marquez subsystem is not implemented and is outside the
final scope. Logical lineage is documented through dbt dependencies/artifacts,
the data model, and architecture documentation, including the chain:

```text
PostgreSQL.orders
    ->
Iceberg Bronze CDC/snapshot state
    ->
dbt Silver
    ->
gold.fact_orders
    ->
analytics.mart_daily_sales
    ->
ClickHouse
```

Preserve that documented lineage when maintaining models or publication paths.
Adding an automated lineage service requires an ADR that explicitly supersedes
ADR 0009.

---

## 35. Git and GitHub workflow

The repository follows a short-lived feature branch workflow.

```text
main
  ^
  |
pull request
  ^
  |
fix/<issue>-description | docs/<issue>-description | chore/<issue>-description
```

Changes should normally go through pull requests.

Prefer:

- focused PRs;
- squash merge;
- Conventional Commits;
- one issue per coherent implementation slice.

Avoid:

- unrelated changes in the same PR;
- formatting the entire repository while fixing one issue;
- generated artifact noise unless required.

### 35.1 Suggested commit types

```text
fix:
refactor:
test:
docs:
ci:
chore:
perf:
```

Use `feat:` only after an accepted ADR explicitly reopens the relevant scope.

Examples:

```text
fix(ingestion): preserve watermark on failed upload
test(dbt): strengthen order payment reconciliation
ci(actions): pin setup action version
docs(runbook): clarify CDC restart recovery
```

---

## 36. GitHub Actions rules

GitHub Actions is the primary CI system.

### 36.1 PR CI

The delivered PR CI includes:

- checkout;
- Python dependency cache;
- ruff check and format validation;
- non-blocking mypy;
- pytest;
- Airflow DAG import/structure tests;
- offline dbt parse;
- Docker Compose config validation.

Do not document a CI check as delivered unless the workflow actually runs it.

### 36.2 Integration validation

Full-stack hosted integration CI is not delivered and must not be claimed.
`make integration` is the documented local entry point against the relevant
live profiles.

If a maintenance task adds a hosted integration workflow, keep the service
subset minimal and always tear down services. A broad CI/CD expansion must be
checked against ADR 0009 before implementation.

### 36.3 Container builds

Custom images should be:

- versioned;
- reproducible;
- scanned where configured;
- published only from trusted events if registry publication is introduced.

Registry publication is not a delivered capability and must not be claimed.
Do not depend only on mutable tags.

### 36.4 Self-hosted runners

A local WSL2 runner is optional.

For a public repository, never allow untrusted fork PR code to run on a local self-hosted runner.

Use GitHub-hosted runners for normal PR validation.
