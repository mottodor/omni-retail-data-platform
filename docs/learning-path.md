# OmniRetail learning path

This guide is for readers who want to study the delivered technologies without
having to infer a route through the entire repository. It uses four neutral
levels: **Overview**, **Code tour**, **Core demo**, and **Full demo**.

The project is a production-like educational capstone, not a production-ready
platform. Its final feature boundary is Phase 8; see
[ADR 0009](adr/0009-freeze-capstone-scope-at-phase-8.md).

## Overview

Start with these artifacts:

1. Read the [README architecture](../README.md#implemented-architecture) to see
   the source-to-BI flow and component responsibilities.
2. Review the [dashboard gallery](../README.md#dashboard-gallery) to see the
   delivered business outputs.
3. Read the [end-to-end capstone flow](../README.md#end-to-end-capstone-flow)
   for the runtime sequence.
4. Use the [data model](data-model.md) for grain, keys, CDC ordering, SCD2, and
   mart semantics.
5. Use the [ADR index](adr/README.md) to understand why major technology and
   lifecycle decisions were made.

The central invariant is:

```text
Iceberg is the analytical source of truth.
ClickHouse is a derived serving layer that can be rebuilt from Iceberg Gold.
```

## Code tour

Follow the stages in order. Each row connects the engineering problem to the
implementation, decision record, and executable evidence.

| Stage | What to learn | Implementation | Decision and evidence |
| --- | --- | --- | --- |
| OLTP source | Deterministic relational fixtures and controlled mutations | [PostgreSQL schema](../postgres/init/01_oltp_schema.sql), [initial generator](../src/omni_retail/generators/oltp/initial.py), [mutation workload](../src/omni_retail/generators/oltp/mutations.py) | [Data model](data-model.md), [generator tests](../tests/unit/generators/oltp/) |
| Batch ingestion | Raw preservation, logical dates, manifests, checksums, quarantine, and bounded retries | [API ingestion](../src/omni_retail/ingestion/api/), [file ingestion](../src/omni_retail/ingestion/files/), [PostgreSQL snapshots](../src/omni_retail/ingestion/postgres_snapshot/) | [Data contracts](data-contracts.md), [ingestion integration tests](../tests/integration/) |
| Iceberg Bronze | Idempotent archive loading plus bounded batch snapshots and CDC data-file/snapshot maintenance | [Bronze loader](../src/omni_retail/lakehouse/bronze/loader.py), [batch snapshot maintenance](../src/omni_retail/lakehouse/snapshot_maintenance.py), [CDC maintenance](../src/omni_retail/lakehouse/cdc_maintenance.py), [table specifications](../src/omni_retail/lakehouse/bronze/specs.py), [Trino catalog](../trino/etc/catalog/iceberg.properties) | [Bronze tests](../tests/unit/lakehouse/bronze/), [CDC maintenance benchmark](benchmarks/cdc-maintenance.md), [maintenance runbooks](runbooks/README.md), [core smoke](../infrastructure/scripts/smoke_core.sh) |
| CDC transport | PostgreSQL WAL capture, persistent Kafka transport, raw event validation, and offset-after-Iceberg delivery | [CDC initialization](../infrastructure/scripts/postgres_cdc_init.sh), [Debezium reconciler](../infrastructure/scripts/debezium_connector_init.py), [Kafka topic initialization](../infrastructure/scripts/kafka_topics_init.sh), [CDC consumer](../src/omni_retail/streaming/cdc/consumer.py) | [ADR 0006](adr/0006-cdc-deployment-and-delivery.md), [CDC tests](../tests/unit/streaming/), [CDC integration test](../tests/integration/test_postgres_cdc.py) |
| dbt modeling | Typed CDC events, delete-aware current state, customer SCD2, Kimball facts/dimensions, marts, and reconciliation | [staging models](../dbt/models/staging/), [intermediate models](../dbt/models/intermediate/), [core models](../dbt/models/core/), [marts](../dbt/models/marts/) | [ADR 0007](adr/0007-cdc-gold-cutover.md), [model semantics](data-model.md) |
| Airflow orchestration | Dataset-triggered batch flows, weekly Iceberg maintenance, and a stable Kafka-offset boundary for analytical refresh | [DAGs](../airflow/dags/), [shared runners](../airflow/include/runners.py), [boundary implementation](../src/omni_retail/streaming/cdc/boundary.py) | [ADR 0008](adr/0008-cdc-analytical-refresh-orchestration.md), [DAG tests](../airflow/tests/test_dags.py) |
| ClickHouse serving | Full-snapshot publication through staging twins and atomic table exchange | [publisher](../src/omni_retail/serving/clickhouse/publisher.py), [serving specifications](../src/omni_retail/serving/clickhouse/specs.py), [migrations](../clickhouse/migrations/) | [ADR 0004](adr/0004-clickhouse-serving-publication.md), [publication integration test](../tests/integration/test_serving_publication.py), [benchmark](benchmarks/phase6-trino-vs-clickhouse.md) |
| Superset BI | Read-only BI connectivity and sanitized dashboards as code | [asset guide](../superset/README.md), [committed assets](../superset/assets/), [bootstrap](../infrastructure/scripts/superset_init.py) | [ADR 0005](adr/0005-superset-deployment.md), [bootstrap integration test](../tests/integration/test_superset_bootstrap.py), [screenshots](screenshots/README.md) |

### Explore without services

The following route does not require a running Docker Compose stack:

```bash
make setup
make test
make dbt-parse
```

These commands validate the Python/unit contracts and the offline dbt project;
they do not claim an end-to-end infrastructure check. Afterward, inspect one
representative chain:

```text
postgres/init/01_oltp_schema.sql
  -> src/omni_retail/streaming/cdc/consumer.py
  -> dbt/models/staging/stg_cdc_orders.sql
  -> dbt/models/intermediate/int_cdc_orders_current.sql
  -> dbt/models/core/fact_orders.sql
  -> dbt/models/marts/mart_daily_sales.sql
  -> src/omni_retail/serving/clickhouse/publisher.py
  -> superset/assets/
```

## Core demo

The Core demo proves the architectural keystone:

```text
Trino -> Polaris -> Iceberg -> MinIO
```

Prerequisites and workstation guidance are in the
[README local setup](../README.md#local-setup). For new local volumes:

```bash
cp .env.example .env      # fill local values; never commit .env
make setup
make up
make generate-oltp
make smoke-core
```

Expected evidence:

- health-gated core services start;
- the deterministic PostgreSQL source is populated;
- Trino creates, writes, and reads an Iceberg table through Polaris;
- Iceberg data is stored in MinIO.

`make generate-oltp` intentionally fails when OLTP tables are already populated.
That guard prevents an accidental destructive reseed. `make reset` is explicitly
destructive and is not part of a routine restart.

On a clean host, `make up` builds the MinIO server and client from pinned,
checksum-verified source commits. The first build needs upstream source and
module access; versions and diagnostics are documented in the
[MinIO image guide](../infrastructure/minio/README.md).

## Full demo

The Full demo follows the delivered portfolio scenario:

```text
PostgreSQL mutation
  -> Debezium
  -> Kafka
  -> Iceberg Bronze
  -> dbt Silver/Gold
  -> ClickHouse
  -> Superset
```

This route needs the complete local resource envelope and all four Compose
profiles. Read the [CDC runbook](runbooks/kafka-cdc.md) before starting: a new
Debezium deployment has a mandatory initial-snapshot gate, and manual dbt or
publication commands must not overlap the coordinated Airflow DAG.

### 1. Bootstrap or restart the source and CDC path

For new local volumes:

```bash
make up
make generate-oltp
make streaming-up
make streaming-status
```

For populated volumes, omit `make generate-oltp`; an already populated source
is the expected routine state.

Follow the runbook's
[first-time bootstrap gate](runbooks/kafka-cdc.md#first-time-data-and-cdc-bootstrap)
before enabling the analytical refresh. The gate verifies connector/tasks,
consumer membership, stable lag-zero offsets, the initial `r` baseline, and a
controlled mutation with a non-null source LSN.

### 2. Start serving and orchestration

```bash
make bi-up
make airflow-up
```

On a first deployment, unpause `transform_lakehouse` only after the bootstrap
gate. Run one controlled refresh through the Airflow UI, or use the documented
DAG test entry point:

```bash
make airflow-dag-test ARGS="transform_lakehouse 2026-10-02"
```

A successful run captures a stable exclusive Kafka-offset boundary, builds dbt
models and tests at that boundary, and then atomically republishes ClickHouse.

### 3. Propagate a controlled source change

```bash
make mutate-oltp EVENTS=30
make streaming-status
```

Wait for the consumer to catch up, then trigger another
`transform_lakehouse` run. Use the inspection queries in the
[CDC runbook](runbooks/kafka-cdc.md#build-and-publish-cdc-backed-gold) to compare
raw history with the selected live state.

### 4. Observe the serving result

Open Superset at `http://127.0.0.1:8088` (or the configured `SUPERSET_PORT`)
and use the local credentials from `.env`. The expected result is:

- create/update/delete events remain auditable in immutable Bronze;
- dbt current state follows PostgreSQL source order and winning deletes;
- critical tests pass before publication;
- ClickHouse changes only through the atomic full-snapshot publisher;
- the dashboards read the refreshed serving snapshot through the read-only
  connection.

For failures, choose the relevant procedure from the
[runbook index](runbooks/README.md). Do not use reset commands as generic
troubleshooting steps.
