# ADR 0006 — CDC deployment and delivery semantics

## Title

Deploy PostgreSQL CDC as a single-node Kafka KRaft + Debezium Kafka Connect
stack and deliver immutable raw events to Iceberg Bronze with at-least-once,
idempotent consumer semantics.

## Status

Accepted.

## Context

Phase 8 (`ROADMAP.md`) replaces polling for the key OLTP entities
`customers`, `orders`, and `payments` with PostgreSQL logical replication via
Debezium and Kafka. ADR 0001 already fixes those technologies; this ADR decides
their local deployment, persistence, event contract, and delivery boundary.
The design is constrained by the following facts:

- The target is a Windows 11 + WSL2 workstation with about 24 GiB available to
  the complete platform. Kafka must therefore run independently in the
  `streaming` Compose profile and must not require ZooKeeper or a multi-broker
  cluster.
- PostgreSQL 16.15 is already the OLTP source. Every selected table has a
  primary key. The existing snapshot ingestion remains the fallback and the
  source for tables outside this slice.
- Kafka Connect must recover connector configuration, source offsets, and task
  status after container restart. Kafka records and Connect state therefore
  need intentional persistent storage rather than container-local files.
- Kafka consumer offset commits alone do not make an external Iceberg write
  exactly once. A crash can occur after the Iceberg commit and before the Kafka
  offset commit, so the Bronze sink must tolerate replay.
- Iceberg remains the analytical source of truth. Bronze must preserve the raw
  Debezium representation and transport metadata without mixing CDC envelopes
  into the existing snapshot-shaped Bronze tables.
- Delete events must remain actionable, while full old-row images are not
  required for the first slice. The source tables' primary keys are sufficient
  to identify deleted business entities.
- The project requires pinned, reproducible dependencies and explicit limits.
  Unbounded replication-slot WAL retention or mutable container tags are not
  acceptable.

The selected compatibility baseline is:

- `apache/kafka:4.3.0`;
- `quay.io/debezium/connect:3.6.1.Final`;
- `confluent-kafka==2.15.1` for the Python consumer, locked in `uv.lock` when
  the implementation adds the dependency.

Debezium 3.6.1's published integration example uses the exact
`quay.io/debezium/connect:3.6.1.Final` image, and its build parent targets
Kafka 4.3.0. The selected Kafka and Debezium image manifests and a Python 3.12
manylinux wheel for `confluent-kafka==2.15.1` were verified during the planning
session. Future upgrades require a compatibility check and an ordinary
versioned change; tags must never float.

## Decision

### Deployment topology and network posture

- Run one Apache Kafka node in KRaft combined `broker,controller` mode in the
  Compose profile `streaming`. Kafka uses a stable non-secret cluster ID and a
  named `kafka-data` volume.
- Provide an internal PLAINTEXT broker listener for Docker-network clients and
  an external PLAINTEXT listener bound only to `127.0.0.1:9092` for local
  diagnostics and integration tests. The KRaft controller listener remains
  internal. This is a local-development posture, not a production security
  model.
- Run one Debezium Kafka Connect worker in distributed mode. Its REST API is
  bound only to `127.0.0.1:8083`. Although there is one worker, distributed
  mode is used so connector configuration, source offsets, and task status are
  persisted in Kafka and survive worker replacement.
- Run one non-root custom `cdc-consumer` image built from the committed
  `uv.lock`. It contains the project package and `confluent-kafka==2.15.1` and
  writes to Iceberg through the existing Trino endpoint.
- Long-running services have real readiness checks and bounded workstation
  memory. One-shot topic, PostgreSQL, and connector reconcilers are gated on
  those checks; no startup step relies on a fixed sleep.

This topology is intentionally single-node and has no high-availability claim.

### PostgreSQL logical replication

Configure the existing PostgreSQL service with:

- `wal_level=logical`;
- `max_replication_slots=4`;
- `max_wal_senders=4`;
- `max_slot_wal_keep_size=2GB`.

An idempotent migration/reconciler, usable with both fresh and existing named
volumes, creates:

- a dedicated LOGIN+REPLICATION role whose password comes from `.env`;
- only the required database `CONNECT`, schema `USAGE`, and table `SELECT`
  grants;
- publication `omni_cdc_publication` containing exactly
  `public.customers`, `public.orders`, and `public.payments`.

The connector uses `pgoutput`, slot `omni_cdc_slot`, the explicit publication,
and `publication.autocreate.mode=disabled`. The slot is persistent and is not
dropped on normal connector shutdown.

All selected tables keep PostgreSQL's default primary-key replica identity.
`REPLICA IDENTITY FULL` is rejected because current-state deletion needs only
the primary key and the extra WAL volume is not justified. Consequently,
non-key old values are not guaranteed on delete events; that limitation is
part of the data contract.

The 2 GiB slot WAL cap protects the workstation from unbounded disk growth.
If Connect remains offline long enough to exceed it, PostgreSQL can invalidate
the slot. This is a fail-loud recovery case documented in the CDC runbook; it
is preferable to silently exhausting local storage.

### Kafka topics and retention

The Debezium topic prefix is `omni.oltp`. The three captured data topics are:

```text
omni.oltp.public.customers
omni.oltp.public.orders
omni.oltp.public.payments
```

Each has one partition, replication factor 1, `cleanup.policy=delete`,
`retention.ms=604800000` (7 days), and `retention.bytes=5368709120` (5 GiB per
partition). Retention occurs when the applicable Kafka limit is reached. One
partition preserves per-table transport order and is adequate for the local
workload; scaling partitions later requires revisiting downstream ordering.

Connect's config, offset, and status topics are created explicitly with one
partition, replication factor 1, `cleanup.policy=compact`, and no time-based
retention. A single worker does not benefit from extra internal-topic
partitions, while explicit creation avoids broker-default replication factors
that are invalid on a one-node cluster.

Debezium transaction/heartbeat topics, when enabled by connector
configuration, follow the same bounded delete-retention policy as data topics
but are not consumed into the three-table Bronze event stream. A 10-second
heartbeat interval makes source progress and idle health observable. Topic
names, limits, and reset commands are recorded in the runbook.

### Debezium connector and initial snapshot

A one-shot reconciler applies the connector through Kafka Connect's REST API
using an idempotent create-or-update operation. The committed configuration
contains no password; source credentials are injected from environment
variables and rendered only in memory. Logs must redact the complete request
body and all secrets.

The connector configuration includes:

- `topic.prefix=omni.oltp`;
- `table.include.list=public.customers,public.orders,public.payments`;
- `snapshot.mode=initial` so a clean clone obtains a complete baseline before
  streaming WAL changes;
- schema-less Kafka Connect JSON converters while retaining Debezium's
  `before`/`after`/`source`/`op` envelope;
- lossless string representation for PostgreSQL decimals;
- transaction metadata and a 10-second heartbeat;
- `tombstones.on.delete=false`.

A delete envelope remains in the data topic and carries the record key needed
to apply the deletion. The additional null-valued compaction tombstone is
unnecessary because data topics use delete retention rather than compaction.

### Bronze event contract

CDC writes to a new append-only table:

```text
iceberg.bronze.postgres_cdc_events
```

It does not write into or update the existing snapshot-shaped
`bronze.customers`, `bronze.orders`, or `bronze.payments` tables. The event
table has one row per consumed Kafka record and contains at least:

- deterministic `event_id`;
- Kafka topic, partition, offset, and record timestamp;
- source schema, table, operation (`r`, `c`, `u`, or `d`), LSN, transaction ID,
  and source timestamp;
- exact decoded key JSON and envelope JSON text;
- extracted `before` and `after` JSON text where present;
- derived event date and ingestion timestamp.

The event date is derived from Debezium's source timestamp, falling back to the
Kafka record timestamp; a record without either is invalid. The table is
partitioned by event date. The unmodified envelope text is the durable raw
representation; extracted fields are routing/indexing conveniences, not a
replacement for it.

The transport identity is:

```text
(kafka_topic, kafka_partition, kafka_offset)
```

`event_id` is a deterministic encoding of that tuple. PostgreSQL LSN is stored
for source ordering and future current-state derivation, but is not the sink's
uniqueness key: several table records can belong to one source transaction,
and Kafka delivery/replay is defined by topic coordinates.

### Consumer delivery boundary

The Python consumer uses a versioned group ID
`omni-iceberg-bronze-cdc-v1`, `auto.offset.reset=earliest`, and
`enable.auto.commit=false`.

For each bounded poll batch it:

1. validates that every record belongs to the configured topic/table allow-list
   and has a supported Debezium envelope;
2. deduplicates transport coordinates inside the batch;
3. writes rows through Trino with an insert-only Iceberg
   `MERGE ... WHEN NOT MATCHED THEN INSERT`, keyed by `event_id`;
4. synchronously commits the corresponding Kafka offsets only after every
   Iceberg write succeeds.

A crash after the Iceberg commit but before offset commit causes Kafka to
redeliver the batch; the insert-only merge finds the same event IDs and makes
that replay a no-op. A write or validation failure does not advance any
covered offset. This is at-least-once transport with an idempotent sink, not an
end-to-end exactly-once guarantee.

Malformed envelopes, unexpected routes, and unsupported operations fail the
consumer partition/batch with structured topic/partition/offset context. They
are not skipped and their offsets are not committed. A DLQ or rejected-object
store is intentionally deferred until Phase 10 failure engineering.

### Lifecycle and reset contract

- Normal stop/restart preserves the Kafka volume, Connect internal topics,
  connector offsets, replication slot, publication, and Bronze table.
- `streaming-reset` is explicitly destructive: it stops the streaming profile,
  removes Kafka/Connect state, and drops the CDC slot/publication before an
  idempotent bootstrap recreates them. It does not delete OLTP data, core
  lakehouse state, or the CDC Bronze table.
- Resetting transport while retaining Bronze causes a new initial snapshot to
  receive new Kafka coordinates and therefore new event IDs. Operators who
  require a clean replay must invoke a separate, explicitly destructive CDC
  Bronze reset before restarting; this separation prevents a transport reset
  from silently deleting analytical history.
- Existing batch snapshots remain enabled during this slice and provide the
  rollback path. Switching dbt Silver/current-state models to CDC is a later
  Phase 8 decision and is outside this ADR's first implementation slice.

## Alternatives considered

- **ZooKeeper-based Kafka:** rejected. KRaft is the current Kafka metadata
  architecture and removes an unnecessary local service.
- **A multi-broker Kafka cluster:** rejected for the workstation profile. It
  would consume materially more memory while the local platform cannot claim
  production HA for its other stateful components either.
- **Debezium Server with file/memory offsets:** rejected. Kafka Connect is the
  roadmap architecture and its distributed mode persists/rebalances connector
  metadata using Kafka's own durable topics.
- **Kafka Connect Iceberg sink:** rejected for this slice. It adds a separate
  connector/plugin and catalog compatibility surface, while custom replay,
  event validation, and Trino/Polaris behavior still need explicit tests. A
  small typed consumer gives direct control over the offset-after-Iceberg
  boundary.
- **Write CDC directly into existing snapshot Bronze tables:** rejected. CDC
  envelopes have a different grain and delete/ordering metadata; mixing them
  would break the established batch contracts and loaders.
- **One CDC Bronze table per source table:** rejected initially. The envelope
  and transport metadata are uniform, while a single raw event ledger keeps
  the ingestion mechanism small. Typed per-entity state belongs in dbt
  staging/Silver.
- **Use source LSN as the only event identity:** rejected. LSN represents
  source ordering, not the concrete Kafka delivery coordinate, and one source
  transaction can produce multiple table records.
- **Commit offsets automatically or before Iceberg writes:** rejected because
  a consumer crash could permanently lose records from Bronze.
- **Claim exactly-once delivery:** rejected. Kafka offset commits and Iceberg
  commits are not one atomic transaction.
- **`REPLICA IDENTITY FULL`:** rejected until a demonstrated use case requires
  complete old-row values. It increases WAL volume for no current-state
  benefit.
- **Avro/Protobuf plus Schema Registry:** deferred. Schema-less JSON preserves
  raw envelopes with fewer services; formal compatibility enforcement belongs
  to the later schema-evolution slice.
- **Start from current WAL without an initial snapshot:** rejected because a
  clean clone would depend on a separate snapshot/bootstrap protocol and could
  leave a capture gap.
- **DLQ in the first slice:** deferred to Phase 10. Fail-stop behavior is
  simpler and prevents silent data loss while the failure taxonomy is still
  being established.
- **SASL/TLS for the local broker:** rejected for this loopback/Docker-network
  deployment. It would add secret/certificate operations without changing the
  local trust boundary; external or shared deployment would require a new
  security decision.

## Consequences

Positive:

- Kafka records, connector state, and consumer progress survive ordinary
  container restarts.
- Raw create/update/delete history is preserved in Iceberg with enough
  transport and source metadata to replay, audit, and later derive current
  state.
- A crash at the Kafka/Iceberg boundary can duplicate delivery attempts but
  not Bronze rows with the same event ID.
- The publication, role, connector, and topics are reproducible and
  idempotently reconciled instead of requiring console-only setup.
- The three-table allow-list and least-privilege source account limit blast
  radius.

Negative and accepted costs:

- The single broker, one partition per table, replication factor 1, and
  PLAINTEXT transport are local-development constraints, not production HA or
  security.
- The initial snapshot emits roughly the current cardinality of all three
  tables and can temporarily load Kafka, Connect, Trino, and Polaris.
- Seven-day/5-GiB data-topic retention bounds replay. Recovery beyond that
  window requires a new snapshot/reset procedure.
- The 2-GiB WAL cap bounds disk use but can invalidate a long-idle slot,
  requiring explicit recovery and a new snapshot.
- Insert-only Iceberg MERGE is heavier than a plain Kafka append and depends on
  Trino/Polaris availability; bounded batches and integration tests are
  required.
- Schema-less JSON defers compatibility enforcement. Unexpected payloads fail
  loudly, but additive/incompatible schema-change policy is not completed in
  this slice.
- Resetting Kafka while retaining Bronze creates a second initial-snapshot
  history with different event IDs; the runbook must distinguish transport
  reset from clean end-to-end replay.

## Rollback / migration considerations

- To disable CDC without data loss, stop the consumer and Connect worker,
  verify/record the final offsets, then remove the `streaming` profile. Existing
  PostgreSQL snapshot ingestion and all downstream models remain unchanged in
  this slice.
- After Connect is stopped, the replication slot can be dropped to release WAL;
  the publication and dedicated role can then be removed by a versioned,
  idempotent rollback. Logical-WAL server settings can be reverted only after
  no logical slots remain and PostgreSQL is restarted.
- Kafka/Connect corruption is recovered by the explicit destructive transport
  reset. For a clean source replay, separately clear only
  `bronze.postgres_cdc_events`, then recreate the slot/connector and allow the
  initial snapshot to complete.
- Changing the topic prefix, partition count, or event-ID algorithm creates a
  new transport identity namespace. Such a migration requires a new consumer
  group/version, a documented Bronze reconciliation strategy, and must not be
  performed in place silently.
- Upgrading Kafka, Debezium, or `confluent-kafka` requires a compatibility
  matrix check, preserved-volume restart test, connector-offset recovery test,
  and source-to-Bronze regression scenario before changing the pinned
  versions.
