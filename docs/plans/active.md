# Active plan — Phase 8 slice 1: restart-safe PostgreSQL CDC to Iceberg Bronze

Task: implement the first bounded slice of Phase 8 from `ROADMAP.md`:
PostgreSQL logical replication -> Debezium -> Kafka -> an immutable Iceberg
Bronze event table for `customers`, `orders`, and `payments`. This is an
execution/resume artifact under AGENTS §41.3; acceptance criteria remain in
`ROADMAP.md`, architectural decisions in ADRs, and status in `PROGRESS.md`.
Delete or replace this file in the implementation-closing commit.

Created: 2026-09-30 (planning session).

## Goal

From a clean clone, start a resource-bounded `streaming` profile, mutate the
three selected OLTP tables, and observe Debezium envelopes in Iceberg Bronze
without a PostgreSQL full scan. The path must be at-least-once and restart-safe:
Kafka/Connect state survives restarts, the consumer commits Kafka offsets only
after durable Bronze writes, and replay of an already-written Kafka record is
a no-op.

This slice is complete when deterministic live tests prove create, update, and
delete capture, duplicate/replay safety, and recovery after service restart.
It establishes raw CDC history only; replacing existing dbt snapshot inputs
with CDC-derived current state belongs to the next Phase 8 slice.

## Context to read first

1. `AGENTS.md` core; `docs/agent/streaming.md`, `ingestion.md`,
   `lakehouse.md`, `platform-ops.md`, `testing-perf.md`, and
   `engineering-practices.md`.
2. `ROADMAP.md` Phase 8, Phase Gate E, profiles/repository layout, and the
   Phase 8 testing scenarios.
3. `PROGRESS.md` current focus and technical debt; open issues #17/#18 are
   explicitly outside this task.
4. `docs/adr/0001-project-architecture.md` and `docs/adr/README.md`.
5. Existing implementation patterns:
   - `docker-compose.yml`, `.env.example`, `Makefile`;
   - `postgres/init/01_oltp_schema.sql` and
     `src/omni_retail/generators/oltp/`;
   - `src/omni_retail/lakehouse/bronze/loader.py`, `specs.py`, and
     `literals.py` for Trino/Iceberg DDL, retries, and SQL safety;
   - `infrastructure/airflow/Dockerfile` and one-shot `*-init` services for
     lockfile-driven images and idempotent bootstrap;
   - `tests/test_compose_guards.py`, `tests/integration/conftest.py`, and
     current Bronze integration tests for fixture ownership and bounded waits.
6. `docs/adr/0006-cdc-deployment-and-delivery.md` — accepted deployment,
   persistence, raw-event, retention, reset, and at-least-once semantics.
7. Upstream references checked during planning via Context7:
   - Debezium 3.6 docs (`/websites/debezium_io_reference_3_6`): `pgoutput`,
     explicit publications, connector configuration, and the 3.6.1 image;
   - Apache Kafka (`/apache/kafka`): Kafka 4.3.0, single-node KRaft, and manual
     offset commit;
   - PostgreSQL 16 (`/websites/postgresql_16`): logical WAL, slots/senders,
     publications, and replica identity.

## Decisions already made

ADR 0006 ratifies the slice boundaries and implementation defaults below.
Change this plan and the ADR explicitly if implementation evidence requires a
materially different architecture.

- The fixed stack already authorizes Kafka + Debezium (ADR 0001); ADR 0006
  documents the significant new deployment and persistence pattern rather
  than reconsidering the stack.
- Local deployment pins `apache/kafka:4.3.0` in KRaft combined
  broker/controller mode and `quay.io/debezium/connect:3.6.1.Final` as one
  Kafka Connect worker, all in Compose profile `streaming`. Kafka data uses a
  named volume. Connect runs in distributed mode even with one worker so
  connector config, offsets, and status live in explicitly created compacted
  Kafka topics and survive worker restarts.
- PostgreSQL gets `wal_level=logical` plus bounded slot/sender/WAL-retention
  settings. A dedicated LOGIN+REPLICATION account receives only the required
  database/schema/table SELECT privileges. An idempotent one-shot migration
  creates an explicit publication for only `public.customers`,
  `public.orders`, and `public.payments`; Debezium must not own all-table
  publication creation.
- All three source tables have primary keys, so use the default PK replica
  identity rather than `REPLICA IDENTITY FULL`. Delete semantics rely on the
  Debezium key and delete envelope; redundant Kafka tombstones are disabled
  because CDC topics use delete retention, not log compaction. Document that
  non-key old values are not guaranteed for deletes.
- Topic prefix is `omni.oltp`; expected data topics are
  `omni.oltp.public.customers`, `.orders`, and `.payments`. Use one partition
  per topic in this laptop deployment to preserve table order, replication
  factor 1, and bounded retention of 7 days or 5 GiB per partition. Internal
  Connect topics are separate, compacted, single-partition, replication factor
  1, and have no time-based retention. PostgreSQL slot WAL is capped at 2 GiB;
  the runbook must explain the resulting replay and invalidated-slot trade-off.
- Connector startup uses an initial snapshot so a clean environment obtains a
  complete baseline before streaming WAL changes. Connector bootstrap is an
  idempotent REST `PUT`/reconcile operation with secrets injected from env;
  no rendered credential-bearing JSON is committed.
- Bronze is a new append-only `iceberg.bronze.postgres_cdc_events` table, not
  a mutation of the existing snapshot-shaped `bronze.customers/orders/payments`
  tables. Each row preserves the original key and envelope JSON plus parsed
  routing metadata: source schema/table, operation, source LSN/transaction/time,
  Kafka topic/partition/offset/timestamp, and ingestion timestamp.
- Transport identity is `(kafka_topic, kafka_partition, kafka_offset)` and is
  also encoded as a deterministic `event_id`. PostgreSQL source LSN is retained
  for source ordering and later current-state logic, but is not the sink's
  uniqueness key.
- The Python consumer uses `enable.auto.commit=false`, validates the allowed
  topic/table mapping and Debezium envelope, writes bounded batches through
  Trino with insert-only `MERGE ... WHEN NOT MATCHED`, then synchronously
  commits offsets. A crash after Iceberg commit but before offset commit
  replays the batch; matching event IDs make it a no-op. A failed Bronze write
  never advances offsets. No exactly-once claim is made.
- The selected client is `confluent-kafka==2.15.1` (Python 3.12 manylinux
  wheel verified during planning), added through `uv` and locked in `uv.lock`.
  The consumer runs in a small lockfile-built custom image as `cdc-consumer`;
  do not put ingestion logic in Kafka Connect transforms or an Airflow DAG.

## Steps

- [x] 1. Created and accepted
  `docs/adr/0006-cdc-deployment-and-delivery.md`; indexed it in
  `docs/adr/README.md`. It pins Kafka 4.3.0, Debezium 3.6.1.Final, and
  confluent-kafka 2.15.1 and ratifies topology, retention, initial snapshot,
  Bronze identity, at-least-once delivery, WAL limits, and reset/rollback.
- [ ] 2. Add PostgreSQL CDC configuration and migration:
  - configure logical WAL, slot/sender capacity, and bounded retained WAL in
    `docker-compose.yml` without changing the pinned PostgreSQL major;
  - add env-driven, idempotent bootstrap for the least-privilege Debezium role
    and three-table publication that works for both fresh and existing named
    volumes;
  - test privileges/publication membership and ensure no credential is logged.
- [ ] 3. Add pinned streaming infrastructure under `kafka/` and
  `infrastructure/scripts/`:
  - Kafka KRaft with internal/external listeners, named data volume,
    workstation-sized memory, and a real readiness check;
  - one-shot topic initializer for the three data topics and Connect internal
    topics with explicit partition/replication/cleanup/retention settings;
  - Debezium Connect with a healthcheck and persistent Kafka-backed state;
  - one-shot connector reconciler with `pgoutput`, explicit slot/publication,
    `table.include.list`, `snapshot.mode=initial`, schema-less JSON envelopes,
    lossless decimal handling, and tombstones disabled.
- [ ] 4. Extend `.env.example` with only required non-secret defaults and
  secret placeholders (replication credentials, host/port/group/batch tuning).
  Add `streaming-up`, `streaming-down`, `streaming-status`, and an explicit
  destructive `streaming-reset` Make target; preserve core volumes unless the
  destructive target says otherwise. Bind any host ports to loopback.
- [ ] 5. Add `src/omni_retail/streaming/cdc/`:
  - typed env config with fail-fast validation;
  - Debezium envelope parser that accepts snapshot/read/create/update/delete,
    preserves raw key/value bytes as JSON text, rejects unknown topics/tables
    or malformed records with topic/partition/offset context, and treats
    broker tombstones explicitly;
  - Iceberg DDL and insert-only MERGE generation for
    `bronze.postgres_cdc_events`, partitioned by a documented event-date
    choice and with deterministic `event_id`;
  - poll/batch loop with bounded batch size/time, structured logging, graceful
    shutdown, retry classification, and synchronous offset commit strictly
    after every durable batch;
  - CLI modes suitable for the long-running Compose service and deterministic
    bounded test runs. Reuse focused Trino primitives where clean; do not force
    the batch-manifest abstractions onto Kafka events.
- [ ] 6. Build a non-root `cdc-consumer` image from `uv.lock`, add the service
  to profile `streaming`, and healthcheck actual liveness/readiness rather than
  process existence alone. Keep startup health-gated; no arbitrary sleeps.
- [ ] 7. Add unit/contract tests:
  - parser coverage for `r/c/u/d`, decimal/timestamp payloads, delete key
    semantics, malformed envelopes, unexpected routes, and tombstones;
  - deterministic identity and Bronze SQL/DDL tests;
  - consumer fake tests proving write-before-commit, no commit on write error,
    replay as no-op, bounded retries, and useful log context;
  - Compose guards for pinned images, `streaming` profile isolation,
    healthchecks, loopback ports, named Kafka volume, documented env vars,
    and one-shot service classification;
  - PostgreSQL bootstrap and connector-config tests that assert the exact
    table allow-list and absence of committed secrets.
- [ ] 8. Add one live integration/smoke scenario with unique fixture IDs and
  bounded polling (never fixed sleeps):
  1. start from healthy core + streaming services and wait for connector
     `RUNNING` plus initial snapshot completion;
  2. insert an isolated customer and pending order/payment; update all three;
     hard-delete the order (including cascaded payment) and then the isolated
     customer;
  3. assert corresponding `c/u/d` rows and source LSNs in Bronze;
  4. rewind/replay a known consumer offset or inject the same parsed record and
     prove one row per event ID;
  5. restart Kafka, Connect, and the consumer with volumes preserved, apply a
     new mutation, and prove no committed event is lost and no prior event is
     duplicated.
  Cleanup only fixture-owned OLTP rows; use a disposable Iceberg schema or
  restore owned objects according to existing integration-test conventions.
- [ ] 9. Document operation and contracts:
  - README start/status/mutate/query flow and resource/profile expectations;
  - `docs/data-model.md` and `docs/data-contracts.md` raw event grain, fields,
    operation/delete semantics, ordering, identity, and snapshot boundary;
  - `docs/runbooks/kafka-cdc.md` connector failure, lag/offset inspection,
    WAL retention/slot risk, replay, restart, and clearly destructive reset;
  - `.env.example`, Make help, and any architecture diagram labels affected.
- [ ] 10. Close the slice: run all validation below, update `PROGRESS.md` to
  Phase 8 `in progress` with the next slice focused on CDC-derived typed/current
  state for dbt, record only lasting limitations/follow-ups, and delete or
  replace this active plan in the same commit.

## Validation

Run and report only commands that actually pass:

```bash
make lint
make test
docker compose config
make up
make streaming-up
make streaming-status
# targeted unit/contract tests added by this slice
uv run pytest tests/unit/streaming tests/test_compose_guards.py -v
# live source -> Debezium -> Kafka -> Bronze + replay/restart scenario
OMNI_INTEGRATION=1 uv run pytest tests/integration/test_postgres_cdc.py -v
make integration
```

Manual acceptance query (exact columns may be adjusted only by ADR 0006):

```sql
SELECT source_table, operation, count(*)
FROM iceberg.bronze.postgres_cdc_events
WHERE source_table IN ('customers', 'orders', 'payments')
GROUP BY 1, 2
ORDER BY 1, 2;
```

Also inspect connector state and consumer-group offsets before and after the
restart test; capture commands and expected output shape in the runbook.

## Scope fence

In scope: logical replication for three tables, Kafka/Debezium persistence,
raw immutable Bronze events, idempotent consumer delivery, restart/replay
proof, tests, and operational docs.

Out of scope for this slice:

- changing dbt Silver/Gold or ClickHouse/Superset to consume CDC;
- disabling the existing seven-table snapshot pipeline;
- CDC for categories, products, order items, or shipments;
- Airflow orchestration, Schema Registry/Avro, DLQ platform, Kafka UI,
  monitoring/Grafana, or Spark;
- solving open MinIO image supply (#17) or supplier-file Bronze loading (#18);
- claiming exactly-once delivery or handling arbitrary incompatible schema
  changes. Additive/incompatible schema evolution and out-of-order current
  state are explicit later Phase 8 slices.
