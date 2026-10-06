# Agent guide — Reliability (quality, contracts, backfills, failures)

Part of the repository agent rules, split out of the monolithic `AGENTS.md`.

Read this guide before working on: data quality checks, data contracts,
backfills, late-arriving data handling, or failure/recovery scenarios.

Precedence: explicit user request > `AGENTS.md` core > accepted ADRs > this
guide > `ROADMAP.md` > existing implementation conventions. On conflict with
the `AGENTS.md` core, the core wins.

Contains original AGENTS.md sections 28–32. Section numbers are preserved so
existing references of the form "AGENTS §N" keep resolving.

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

Important delivered datasets should have contracts containing, where the
current adapter and source semantics support them:

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
- API corrections;
- captured CDC events.

Do not assume source events always arrive in event-time order. For CDC-derived
state, distinguish source ordering (for example PostgreSQL LSN and Kafka
offset) from event, ingestion, and processing timestamps.

---

## 32. Failure engineering

Delivered paths require explicit failure behavior for scenarios such as:

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
- diagnostic log, health, or status signal;
- recovery steps.

Do not hide failure to keep a demo green.
