# Agent guide — Testing and performance

Part of the repository agent rules, split out of the monolithic `AGENTS.md`.

Read this guide before working on: unit/integration/E2E tests, test fixtures,
or performance benchmarks.

Precedence: explicit user request > `AGENTS.md` core > accepted ADRs > this
guide > `ROADMAP.md` > existing implementation conventions. On conflict with
the `AGENTS.md` core, the core wins.

Contains original AGENTS.md sections 37–39. Section numbers are preserved so
existing references of the form "AGENTS §N" keep resolving.

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
- ClickHouse `ORDER BY`;
- ClickHouse query latency;
- batch size;
- object file size.

Benchmark documentation should include enough detail to reproduce the result.
