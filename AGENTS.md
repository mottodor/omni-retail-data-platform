# AGENTS.md — OmniRetail Data Platform

This file defines the operating rules for AI coding agents working on **OmniRetail Data Platform**.

The repository is a production-like Data Engineering portfolio project. The goal is not to maximize the number of technologies used. The goal is to build a reproducible, testable, observable, and explainable data platform that demonstrates commercial-grade engineering practices.

---

## 1. Authority and source of truth

Before changing code, always read:

1. `AGENTS.md` — mandatory engineering and agent rules.
2. `ROADMAP.md` — implementation order, scope, acceptance criteria, and target architecture.
3. Relevant files under `docs/adr/` — architectural decisions already made.
4. Relevant tests and current implementation.

When instructions conflict, use this precedence:

1. Explicit user request for the current task.
2. `AGENTS.md`.
3. Accepted ADRs.
4. `ROADMAP.md`.
5. Existing implementation conventions.

Do not silently override an accepted architectural decision.

If an architectural change is required, create or update an ADR before implementing the change.

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
- dbt Core.
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
2. Read `AGENTS.md`.
3. Read the relevant section of `ROADMAP.md`.
4. Read relevant ADRs.
5. Inspect existing implementation.
6. Inspect existing tests.
7. Identify the smallest correct scope.
8. Identify affected services, contracts, schemas, and documentation.
9. Check whether the task requires a new dependency or architectural decision.
10. If a new technology or architecture decision is required, create an ADR first.
11. When checking upstream documentation for any technology (Trino, dbt, Airflow, Iceberg, ClickHouse, Superset, Debezium, Spark, etc.), use the `context7` MCP tools.

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
- `.env.example` is updated when configuration changes;
- logs expose useful execution context;
- failure behavior is defined;
- manual verification steps are documented;
- no unrelated refactoring was included.

---

## 7. Repository structure

Follow the repository structure defined by `ROADMAP.md`.

Expected top-level layout:

```text
.github/
airflow/
dbt/
ingestion/
generators/
spark/
kafka/
trino/
clickhouse/
postgres/
superset/
observability/
infrastructure/
tests/
docs/
docker-compose.yml
docker-compose.override.yml
Makefile
pyproject.toml
.env.example
AGENTS.md
ROADMAP.md
README.md
```

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

## 9. Logging requirements

Pipeline logs should expose useful execution context.

Where applicable include:

- `run_id`;
- `batch_id`;
- dataset name;
- source name;
- logical date/data interval;
- object/file name;
- row count;
- rejected row count;
- checksum;
- partition;
- Kafka topic/partition/offset where relevant.

Avoid logging full raw records when they may contain sensitive or large data.

Prefer summaries and identifiers.

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

## 14. Data ingestion rules

### 14.1 Raw data preservation

For external sources, preserve the raw representation before transformation whenever practical.

REST API:

```text
API response -> landing/raw -> normalized Bronze
```

Files:

```text
landing -> processing -> archive
                       -> rejected
```

Do not transform away the original input before durable raw storage unless explicitly justified.

### 14.2 Ingestion metadata

Include metadata where applicable:

- source;
- ingestion timestamp;
- batch ID;
- source file/object;
- checksum;
- schema version;
- extraction interval;
- source cursor/watermark.

### 14.3 Incremental extraction

Do not use unbounded production-like:

```sql
SELECT *
FROM source_table
```

for every scheduled run.

Use a documented strategy such as:

- timestamp watermark;
- monotonic ID;
- snapshot;
- CDC.

Initial bootstrap extraction may use full snapshots when appropriate.

### 14.4 Idempotency

A retry or replay of the same logical batch must not create duplicate business state.

Idempotency strategy must be explicit.

Possible mechanisms:

- deterministic object path;
- batch manifest;
- checksum;
- unique ingestion key;
- MERGE semantics;
- deduplication key;
- replace partition;
- transaction boundaries.

---

## 15. REST API ingestion

API clients must consider:

- pagination;
- request timeout;
- retry;
- exponential backoff;
- rate limiting;
- HTTP 429;
- HTTP 5xx;
- malformed responses;
- schema validation;
- partial responses;
- checkpoint/watermark;
- backfill.

Do not implement infinite retry loops.

Retries must have:

- maximum attempts or timeout;
- retryable/non-retryable classification;
- logging.

Raw responses should be persisted when required by the ingestion design.

Mock APIs are acceptable and encouraged for deterministic development and fault injection.

---

## 16. File/S3 ingestion

Support production-like file handling.

Expected object flow:

```text
landing
   ->
processing
   ->
archive

invalid
   ->
rejected
```

Important concerns:

- duplicate file detection;
- checksum validation;
- schema validation;
- malformed rows;
- partially uploaded objects;
- deterministic archive paths;
- backfill;
- reprocessing.

Do not silently skip malformed files.

Quarantined files require an explicit reason.

---

## 17. PostgreSQL rules

PostgreSQL serves as an OLTP source and may also host component metadata databases where appropriate.

OLTP schema should use:

- primary keys;
- foreign keys where realistic;
- constraints;
- meaningful data types;
- `created_at`;
- `updated_at`;
- realistic states/statuses.

The source generator must support:

- deterministic seed;
- initial load;
- inserts;
- updates;
- deletes.

Database migrations should be versioned.

Do not manually mutate production-like schema outside migrations unless the task is explicitly a schema-evolution exercise.

---

## 18. Kafka and Debezium rules

Do not introduce Kafka before the roadmap phase that requires it.

Kafka is used for:

- CDC transport;
- application events;
- clickstream.

Debezium is used for PostgreSQL CDC.

CDC implementation must explicitly handle:

- create;
- update;
- delete;
- duplicate delivery;
- restart;
- offset recovery;
- ordering limitations;
- schema changes;
- replay.

For CDC-derived state, define how event identity is determined.

Do not assume exactly-once semantics without proving the full end-to-end guarantee.

Prefer designing for at-least-once delivery plus idempotent consumers.

Document topic naming and retention strategy.

---

## 19. Airflow rules

Airflow baseline is Apache Airflow 2.11.2.

### 19.1 Airflow responsibility

Airflow coordinates work.

Airflow DAG files must not become transformation code repositories.

Do not embed large business SQL transformations inside DAGs when the SQL belongs in dbt.

Airflow should orchestrate:

- extraction;
- loading;
- dbt execution;
- publishing;
- maintenance;
- data quality;
- recovery workflows.

### 19.2 DAG design

Prefer:

- TaskFlow API where appropriate;
- small tasks;
- explicit dependencies;
- isolated failure domains;
- pools for constrained external systems;
- timeouts;
- retry policy;
- logical date/data interval;
- parameterized backfills.

Avoid:

- mega-DAGs;
- one task doing ingestion + transformation + publication;
- network calls at DAG import time;
- database connections at DAG import time;
- dynamic behavior that breaks deterministic DAG parsing.

### 19.3 Airflow configuration

Use:

- environment variables or proper config;
- Airflow Variables only for non-secret runtime configuration;
- a secret-backend pattern or env for secrets.

### 19.4 DAG tests

Every new DAG should have import/parse validation.

Where useful, test:

- task IDs;
- dependencies;
- schedule;
- retries;
- required params;
- absence of import errors.

---

## 20. dbt rules

dbt owns relational analytical transformations.

Expected model layers:

```text
staging
intermediate
core
marts
```

### 20.1 Staging

Responsibilities:

- source renaming;
- basic casting;
- standard naming;
- minimal cleanup.

Do not place large business logic here.

### 20.2 Intermediate

Responsibilities:

- deduplication;
- joins;
- normalization;
- reusable business preparation.

### 20.3 Core

Responsibilities:

- facts;
- dimensions;
- conformed entities;
- SCD logic.

### 20.4 Marts

Responsibilities:

- business-facing datasets;
- KPI-ready structures;
- BI-oriented grain.

### 20.5 Model contracts

Every important model must document:

- purpose;
- grain;
- primary key or uniqueness expectation;
- important dimensions;
- important measures;
- upstream dependencies.

### 20.6 dbt tests

Use built-in tests where appropriate:

- `unique`;
- `not_null`;
- `relationships`;
- `accepted_values`.

Add custom business tests such as:

- non-negative financial values;
- order/payment reconciliation;
- valid temporal ordering;
- uniqueness at declared grain.

A failing business-critical test should fail the relevant pipeline unless the expected policy explicitly quarantines the data.

---

## 21. Analytical modeling rules

The primary Gold model is Kimball-style.

Expected examples:

Dimensions:

- `dim_customer`;
- `dim_product`;
- `dim_date`;
- `dim_campaign`.

Facts:

- `fact_orders`;
- `fact_order_items`;
- `fact_payments`;
- `fact_shipments`.

Marts:

- `mart_daily_sales`;
- `mart_customer_ltv`;
- `mart_marketing_roi`;
- `mart_delivery_performance`.

Every fact table must define its grain explicitly.

Do not mix grains in one fact table without a documented reason.

### 21.1 SCD Type 2

`dim_customer` should demonstrate SCD Type 2.

Expected concepts:

- surrogate key;
- business key;
- valid-from;
- valid-to;
- current-row indicator.

Tests should verify:

- no overlapping validity intervals;
- one current row per business key;
- historical versions are retained.

### 21.2 Data Vault

Data Vault is a later mini-domain exercise only.

Do not replace the primary Gold Kimball model with Data Vault.

---

## 22. Iceberg rules

Iceberg is the persistent analytical source of truth.

Use Iceberg for Bronze, Silver, and Gold datasets where defined by architecture.

Important engineering topics include:

- snapshots;
- schema evolution;
- partition evolution;
- time travel;
- rollback;
- compaction;
- orphan files;
- snapshot expiration;
- small-files problem.

### 22.1 Bronze

Bronze should stay close to source representation.

Add ingestion metadata.

Avoid unnecessary business transformations.

### 22.2 Silver

Silver should contain:

- cleaned;
- typed;
- deduplicated;
- normalized;
- merge-ready/current-state or clearly historical datasets.

### 22.3 Gold

Gold contains business models and marts.

### 22.4 Maintenance

Do not run destructive maintenance with unsafe retention defaults.

Maintenance jobs must:

- be parameterized;
- have documented retention;
- be tested with non-production fixtures;
- log affected tables/snapshots/files.

---

## 23. Polaris rules

Apache Polaris is the Iceberg REST catalog.

Do not bypass Polaris with an unrelated catalog implementation without an ADR.

Configuration must be reproducible from Docker Compose and committed config.

Credentials remain externalized.

---

## 24. Trino rules

Trino is the primary SQL engine for Iceberg and federated analytical access.

Use Trino for:

- Iceberg SQL;
- dbt execution;
- ad-hoc federation;
- comparison against ClickHouse serving performance.

Do not rely on cross-system federation as a substitute for proper ingestion when persistent analytical data is required.

### 24.1 Query quality

For important or slow queries:

- inspect `EXPLAIN`;
- consider partition pruning;
- predicate pushdown;
- join strategy;
- scan volume;
- memory pressure.

Avoid unnecessary:

```sql
SELECT *
```

in stable analytical models.

Select required columns explicitly.

---

## 25. Spark rules

Spark is not a default transformation engine for the project.

Use Spark only where it demonstrates a meaningful distributed-processing case, such as:

- clickstream sessionization;
- large backfills;
- expensive distributed joins;
- large file transformations.

Do not use Spark for a dataset that is trivially handled by Trino/dbt/Python unless the issue is specifically a Spark learning benchmark.

Spark jobs should consider:

- partition count;
- shuffle;
- broadcast join;
- skew;
- event time;
- late events;
- deterministic tests;
- memory limits appropriate for the laptop.

For benchmark tasks, capture `explain` output and before/after metrics.

---

## 26. ClickHouse rules

ClickHouse is a low-latency serving layer.

It is not the master analytical store.

### 26.1 Rebuildability

Every ClickHouse serving dataset must be reconstructable from Iceberg Gold.

### 26.2 Table design

Choose intentionally:

- engine;
- `ORDER BY`;
- partitioning;
- primary key semantics;
- TTL.

Do not add partitioning simply because the feature exists.

### 26.3 Publication

Gold -> ClickHouse publication must be:

- repeatable;
- idempotent;
- recoverable;
- measurable.

Document whether publication is:

- append;
- replace partition;
- upsert;
- full rebuild.

### 26.4 Permissions

Superset uses a read-only account.

Do not grant BI write permissions.

---

## 27. Superset rules

Superset is the main business BI tool.

Primary connection:

```text
Superset -> ClickHouse
```

Secondary exploratory connection:

```text
Superset -> Trino
```

Use Docker-network hostnames.

Do not require public exposure of ClickHouse for normal local use.

Store exportable Superset metadata in the repository where practical.

Expected dashboards eventually include:

- Executive;
- Sales;
- Customer;
- Marketing;
- Funnel.

Dashboard queries should primarily use prepared serving marts rather than expensive raw queries.

---

## 28. Data quality rules

Data quality exists at multiple layers.

### 28.1 Source quality

Validate:

- schema;
- required fields;
- source volume;
- freshness.

### 28.2 Pipeline quality

Validate:

- duplicates;
- completeness;
- row counts;
- referential consistency;
- ingestion status.

### 28.3 Business quality

Validate:

- order/payment reconciliation;
- non-negative values;
- valid timestamps;
- delivery constraints;
- KPI consistency.

Do not silently downgrade critical quality failures to warnings.

The handling policy must be explicit:

- fail;
- quarantine;
- warn;
- continue.

---

## 29. Data contracts

Important datasets should eventually have contracts containing:

- owner;
- schema;
- types;
- nullability;
- grain;
- freshness SLA;
- compatibility policy.

Schema changes must classify compatibility.

Do not silently make breaking changes to downstream models.

For breaking changes:

1. document impact;
2. update contract;
3. update tests;
4. update downstream dependencies;
5. include an ADR if architecture is affected.

---

## 30. Backfill rules

Every scheduled or incremental pipeline should define its backfill behavior.

A good backfill is:

- date/range parameterized;
- idempotent;
- restartable;
- observable;
- rate/load controlled.

Do not write a separate unrelated script for backfill if the same pipeline can support historical intervals cleanly.

Avoid logic based on wall-clock `now()` when Airflow logical dates/data intervals should be used.

---

## 31. Late-arriving data

Pipelines must explicitly define behavior for late-arriving:

- facts;
- dimensions;
- clickstream events;
- API corrections.

Do not assume source events always arrive in event-time order.

For streaming/sessionization tasks, distinguish:

- event time;
- processing time.

---

## 32. Failure engineering

The project intentionally includes failures.

Implement explicit handling for scenarios such as:

- API 429;
- API 500;
- request timeout;
- malformed CSV;
- missing object;
- duplicate file;
- Kafka duplicate;
- consumer restart;
- ClickHouse unavailable;
- dbt test failure;
- Trino failure;
- schema evolution;
- partial batch.

For every significant failure mode, define:

- expected behavior;
- retry policy;
- idempotency behavior;
- quarantine or DLQ behavior;
- alert/observability signal;
- recovery steps.

Do not hide failure to keep a demo green.

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

---

## 37. Testing strategy

Follow the testing pyramid.

```text
                 E2E
              /       \
       integration tests
      /                 \
unit + dbt + contract tests
```

### 37.1 Unit tests

Use for:

- API clients;
- parsers;
- validators;
- data generators;
- deterministic transformations;
- Spark transformation helpers;
- config parsing.

### 37.2 Integration tests

Use for boundaries such as:

```text
PostgreSQL -> ingestion
MinIO -> Iceberg -> Trino
dbt -> Trino
Gold -> ClickHouse
Debezium -> Kafka
Superset connectivity
```

### 37.3 E2E tests

Target business path:

```text
source mutation
    ->
CDC/batch
    ->
Iceberg
    ->
dbt
    ->
ClickHouse
    ->
KPI query
```

E2E tests should be few, high-value, and deterministic.

---

## 38. Test data rules

Tests should use deterministic fixtures.

Prefer:

- fixed random seeds;
- explicit timestamps;
- compact representative datasets;
- isolated schema/database names;
- cleanup after test.

Avoid depending on arbitrary current dates unless the behavior specifically tests current time.

Do not make unit tests depend on external public APIs.

Use mock/fake services for deterministic tests.

---

## 39. Performance engineering

Do not optimize without evidence.

When a performance issue or benchmark is part of the task:

1. define the workload;
2. record baseline;
3. make one intentional change;
4. rerun;
5. record result;
6. explain trade-offs.

Relevant topics:

- Trino scan volume;
- Iceberg partition pruning;
- small files;
- Spark shuffle;
- ClickHouse `ORDER BY`;
- ClickHouse query latency;
- batch size;
- object file size.

Benchmark documentation should include enough detail to reproduce the result.

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

## 44. SQL style

Prefer readable SQL.

Example:

```sql
select
    order_id,
    customer_id,
    order_total,
    created_at
from {{ ref('stg_orders') }}
where is_deleted = false
```

Guidelines:

- one selected column per line for non-trivial queries;
- explicit aliases;
- meaningful CTE names;
- avoid deeply nested subqueries when CTEs improve readability;
- document non-obvious business logic;
- avoid dialect-specific tricks unless justified;
- do not use ordinal `GROUP BY 1,2,3` in persistent models.

Every analytical model must have a clear grain.

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

## 46. Generated data

The synthetic data generator is part of the product.

It should produce plausible data for:

- customers;
- products;
- categories;
- orders;
- order items;
- payments;
- shipments;
- marketing;
- clickstream.

Use deterministic seeds where test reproducibility matters.

The generator should eventually support continuous mutations.

Avoid creating random values that violate database constraints unless testing invalid data explicitly.

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

Recommend the next logical GitHub issue, but do not implement it unless requested.

---

## 50. Recommended task prompt

A task sent to an AI coding agent can use this template:

```text
Implement GitHub issue <N> for OmniRetail Data Platform.

Before changing code:
1. Read AGENTS.md, ROADMAP.md, and relevant ADRs.
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

---

## 53. Final rule

Every technology must solve a concrete engineering problem.

When choosing between:

- a simpler implementation that is correct, testable, reproducible, and aligned with the architecture;
- a more complex implementation that merely demonstrates another tool;

choose the simpler implementation.

Complexity should be introduced only when the roadmap creates a real requirement for it.
