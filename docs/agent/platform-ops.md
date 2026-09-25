# Agent guide — Platform operations (Docker, Make, Git, CI, observability, lineage)

Part of the repository agent rules, split out of the monolithic `AGENTS.md`.

Read this guide before working on: `docker-compose.yml`, service profiles and
healthchecks, the `Makefile`, Git branches/commits/PRs, GitHub Actions
workflows, Prometheus/Grafana observability, or OpenLineage/Marquez lineage.

Precedence: explicit user request > `AGENTS.md` core > accepted ADRs > this
guide > `ROADMAP.md` > existing implementation conventions. On conflict with
the `AGENTS.md` core, the core wins.

Contains original AGENTS.md sections 12–13 and 33–36. Section numbers are
preserved so existing references of the form "AGENTS §N" keep resolving.

---

## 12. Docker and Docker Compose rules

Docker Compose is the primary local orchestration mechanism.

### 12.1 Compose profiles

Preserve logical profiles:

- `core`;
- `orchestration`;
- `streaming`;
- `spark`;
- `observability`;
- `bi`.

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

Spark, Kafka, Trino, Airflow, Superset, Grafana, and Marquez do not need to run simultaneously during early phases.

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

Expected commands should evolve toward:

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
make deploy
```

Commands should be:

- discoverable;
- documented;
- composable;
- safe by default.

Do not hide destructive operations under harmless names.

---

## 33. Observability rules

Prometheus and Grafana are used for platform observability.

Expected metrics eventually include:

- DAG success/failure/duration;
- data freshness;
- Kafka consumer lag;
- Trino latency/errors/memory;
- ClickHouse latency;
- PostgreSQL connections/WAL;
- MinIO storage/object metrics.

Dashboards should help answer:

- what failed?
- when?
- which dataset is stale?
- what is the upstream cause?
- what is the downstream impact?

---

## 34. Lineage rules

OpenLineage + Marquez are used for lineage.

The target lineage should eventually make a chain similar to this visible:

```text
PostgreSQL.orders
    ->
bronze.orders
    ->
silver.orders
    ->
fact_orders
    ->
mart_daily_sales
    ->
ClickHouse
```

Do not add lineage instrumentation before core pipelines are stable unless the roadmap phase explicitly calls for it.

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
feature/<issue>-description
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
feat:
fix:
refactor:
test:
docs:
ci:
chore:
perf:
```

Examples:

```text
feat(ingestion): add supplier CSV validator
test(dbt): add order payment reconciliation test
ci(actions): add core integration workflow
docs(adr): document ClickHouse serving strategy
```

---

## 36. GitHub Actions rules

GitHub Actions is the primary CI system.

### 36.1 PR CI

Expected checks eventually include:

- checkout;
- Python dependency cache;
- ruff;
- mypy;
- pytest unit;
- Airflow DAG import tests;
- dbt parse/compile;
- Docker Compose config validation.

### 36.2 Integration CI

Integration workflows may start a minimal infrastructure subset on GitHub-hosted Linux runners.

Keep the subset minimal.

Always tear down services.

### 36.3 Container builds

Custom images should be:

- versioned;
- reproducible;
- scanned where configured;
- pushed to GHCR after trusted merge/tag events.

Do not depend only on mutable tags.

### 36.4 Self-hosted runners

A local WSL2 runner is optional.

For a public repository, never allow untrusted fork PR code to run on a local self-hosted runner.

Use GitHub-hosted runners for normal PR validation.
