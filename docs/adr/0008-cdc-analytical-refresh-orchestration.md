# ADR 0008 — Stable-boundary CDC analytical refresh orchestration

## Title

Coordinate CDC-backed dbt and ClickHouse refreshes in one Airflow DAG pinned to
a stable Kafka consumer boundary.

## Status

Accepted.

## Context

ADR 0006 establishes a long-running, restart-safe consumer that writes immutable
Debezium events for `customers`, `orders`, and `payments` to
`iceberg.bronze.postgres_cdc_events`. The consumer writes an idempotent Iceberg
MERGE before it synchronously commits Kafka offsets. ADR 0007 makes that ledger
authoritative for typed Silver state and the customer/order/payment Gold models.

The remaining Phase 8 gap is orchestration. CDC Bronze advances continuously,
but the existing Airflow graph is batch-oriented:

```text
raw dataset -> load_bronze -> transform_lakehouse -> GOLD dataset -> publish_serving
```

The CDC consumer does not emit an Airflow Dataset event. Operators must
currently inspect consumer lag, run a full dbt build, and then publish the marts
to ClickHouse manually. This leaves no visible, repeatable Airflow path from a
source mutation to the serving layer.

Starting dbt merely because the Kafka consumer reports lag zero is insufficient.
A new record can arrive after the lag check and while dbt is building its model
graph. The three CDC staging models are views over the raw ledger, so an
unbounded run could let different downstream tables observe different moving
frontiers. Orders and payments also use different Kafka topics; ADR 0007 keeps
business reconciliation fail-loud because Kafka offsets cannot be compared
across topics.

Keeping transformation and publication in separate DAGs creates another race.
`max_active_runs` is enforced per DAG, not across DAGs. A later transform run can
replace Iceberg Gold while the independent publication DAG is still copying the
previous logical refresh to ClickHouse. A shared pool reduces concurrency but
does not express the required end-to-end ordering as clearly as one DAG run.

The design is constrained by these facts:

- Airflow remains pinned to 2.11.2 and coordinates work without owning business
  SQL (ADR 0003 and the Airflow agent guide).
- Airflow 2.11 supports `DatasetOrTimeSchedule`, which can preserve the existing
  Bronze dataset trigger while adding a periodic CDC refresh.
- The accepted Kafka topology has exactly one partition for each of the three
  captured table topics.
- `confluent-kafka` is already a pinned project dependency and exposes consumer
  group offsets, group membership, topic metadata, and broker watermarks.
- Kafka high watermarks are exclusive: the high watermark is the last record's
  offset plus one.
- ClickHouse publication is already an idempotent full-snapshot staging-table
  swap; the previous serving snapshot remains visible if publication fails
  (ADR 0004).
- Full dbt builds and full mart publication are acceptable at the current local
  workstation scale. No measured need yet justifies another checkpoint store or
  incremental publication protocol.
- A new Airflow DAG starts paused. The first Debezium initial snapshot needs an
  explicit bootstrap gate before automatic refreshes are enabled.

This decision changes the orchestration strategy and therefore requires an ADR.
It does not change CDC delivery identity, source-of-truth ownership, Gold
business semantics, or ClickHouse table schemas.

## Decision

### Coordinated DAG

Keep the existing Airflow DAG ID `transform_lakehouse`, but make it the single
coordinated analytical refresh workflow:

```text
wait_for_cdc_boundary
    -> dbt_build
    -> publish_serving
```

Each step remains a separate Airflow task. Boundary logic and runnable pipeline
code live in the `omni_retail` package or thin reusable runners; transformation
SQL remains in dbt.

The DAG uses Airflow 2.11's `DatasetOrTimeSchedule` and starts when either:

- `lakehouse://bronze` is updated by the existing batch Bronze loader; or
- the hourly UTC cron `0 * * * *` fires.

The dataset trigger preserves prompt rebuilding after snapshot/API Bronze
updates. The hourly trigger detects CDC progress without coupling the
long-running consumer to the Airflow API or adding a Kafka-trigger provider.
The DAG keeps `catchup=False` and `max_active_runs=1`, which serializes the
boundary, build, and publication tasks across all scheduled runs of this DAG.

Retire the standalone `publish_serving` DAG and remove the intermediate
orchestration Dataset event `lakehouse://gold`. This removes only the Airflow
handoff; Iceberg Gold remains the source of truth. Host-side publication CLI and
Makefile commands remain available for recovery and diagnostics.

Operators must not run a manual dbt build or serving publication concurrently
with an active coordinated DAG run. Airflow serialization cannot protect Gold
from an unrelated host process.

### Stable consumer boundary

Before dbt starts, `wait_for_cdc_boundary` performs bounded polling and requires
all of the following in each candidate sample:

1. Kafka Connect reports connector `omni-postgres-cdc` as `RUNNING`.
2. Every reported connector task is `RUNNING`.
3. Consumer group `omni-iceberg-bronze-cdc-v1` (or its configured versioned
   value) has an active member.
4. The exact allow-listed data topics exist:

   ```text
   omni.oltp.public.customers
   omni.oltp.public.orders
   omni.oltp.public.payments
   ```

5. Every topic has exactly partition `0`; missing or additional partitions are
   incompatible with the accepted ordering contract.
6. Each partition has a valid committed consumer-group offset.
7. Each committed offset equals the broker high watermark, so lag is zero.
8. Two consecutive lag-zero samples have identical high watermarks for all
   three topics and are separated by the configured stability window.

Any unhealthy component, absent/invalid offset, unexpected topology, non-zero
lag, or watermark movement invalidates the candidate sample. Polling continues
within the bounded wait; it never degrades to a warning or silently proceeds.
Exhausting the wait fails the Airflow task with per-topic committed/high/lag
context, after which normal bounded Airflow retries apply.

Defaults are:

| Setting | Value |
|---|---:|
| Stability window | 10 seconds |
| Total boundary wait | 5 minutes |
| Kafka/Connect request timeout | 10 seconds |
| Airflow boundary retries | 3 |
| Initial retry delay | 1 minute, exponential backoff |
| dbt task execution timeout | 60 minutes |
| publication task execution timeout | 30 minutes |

Boundary timing and network addresses are validated environment configuration.
The Airflow containers use Docker-network addresses for Kafka and Kafka Connect.
No connector configuration endpoint is fetched or logged because its response
contains the PostgreSQL replication password.

The result is an XCom-safe mapping with one entry per exact topic:

```json
{
  "omni.oltp.public.customers": {
    "partition": 0,
    "offset_exclusive": 101
  },
  "omni.oltp.public.orders": {
    "partition": 0,
    "offset_exclusive": 205
  },
  "omni.oltp.public.payments": {
    "partition": 0,
    "offset_exclusive": 303
  }
}
```

The captured boundary is a quiescent transport frontier. It does not create a
cross-topic Kafka order, a distributed transaction, or an exactly-once claim.
PostgreSQL LSN remains the authoritative source-order field inside each typed
entity history, and existing dbt reconciliation continues to fail a partial or
invalid business state.

### Boundary-pinned dbt build

The Airflow dbt task must receive the captured boundary as a JSON `--vars`
argument. A dbt macro applies the matching static predicate to each CDC staging
view:

```sql
kafka_partition = 0
and kafka_offset < offset_exclusive
```

The exclusive comparison matches Kafka's high-watermark semantics. The runner
validates exact topic coverage, partition `0`, and non-negative integer offsets
before starting the dbt subprocess. It passes JSON as one subprocess argument;
no shell interpolation is used.

Consequently, Bronze records written after boundary capture cannot enter that
dbt run, even while the consumer continues running. Current-state selection and
customer SCD2 still use source LSN and the existing within-topic Kafka offset
tie-breakers defined by ADR 0007.

The Airflow path always supplies a boundary. The generic host-side
`make dbt-build` path remains available for isolated schemas, tests, and
operator diagnostics; against the normal CDC-backed schemas it is a manual path
that requires the runbook's explicit stable-lag check. It must not run in
parallel with the coordinated DAG.

### Publication and failure semantics

`publish_serving` calls the existing full rebuild publisher only after the
boundary-pinned dbt build, including all dbt tests, succeeds. The ClickHouse
staging-table swap and mart contracts do not change.

Failure behavior is explicit:

| Failure | Result |
|---|---|
| Connect is unavailable or not running | Boundary task waits, then fails/retries within its bounds |
| CDC consumer is inactive | Boundary task waits, then fails/retries |
| Lag remains non-zero or offsets keep moving | No dbt task starts; bounded wait eventually fails |
| dbt model or test fails | Publication does not start |
| ClickHouse is unavailable | Iceberg Gold remains authoritative; the prior serving snapshot remains visible and publication retries |
| A task retry occurs | The idempotent task is rerun; CDC Bronze is not reset |
| The whole DAG is rerun | A fresh boundary is captured, then the full build and full publication are repeated |

A publication-task retry in the same DAG run uses the successfully built current
Gold state. `max_active_runs=1` prevents a later Airflow refresh from replacing
Gold while that publication is retrying.

The hourly run performs the full dbt build and ClickHouse snapshot swap even if
its captured boundary equals the previous run. This intentionally avoids a new
Airflow Variable/XCom lookup contract or Iceberg checkpoint table. The repeated
work is idempotent and acceptable at current scale; skipping unchanged
boundaries can be considered later only after measuring runtime and resource
cost.

### Initial snapshot gate and profile lifecycle

The coordinated DAG remains paused during first-time CDC bootstrap. Before the
first unpause, the operator follows the CDC runbook to verify that:

- the connector and consumer are healthy;
- Debezium's initial snapshot has completed;
- the three data topics have reached a stable lag-zero boundary; and
- the Bronze ledger contains the expected initial `r` baseline.

This is a documented bootstrap acknowledgment, not a recurring manual pipeline
step. Ordinary service restarts preserve connector offsets, Kafka data, and the
replication slot, so they do not repeat the gate. Automated JMX-based snapshot
completion detection is deferred to the observability phase rather than adding
metrics infrastructure to Phase 8.

Airflow does not start or stop Kafka, Connect, the consumer, Trino, or
ClickHouse. Operators start the profiles in this order:

```text
core -> streaming -> bi -> orchestration
```

There are no hard cross-profile Compose `depends_on` edges and no Docker socket
inside Airflow. A missing runtime dependency fails loudly through the boundary,
dbt, or publication task and is recovered using the relevant runbook.

## Alternatives considered

- **Keep the manual lag check, dbt build, and serving publish only:** rejected.
  It leaves the Phase 8 end-to-end path outside Airflow and cannot demonstrate a
  visible, repeatable source-to-serving orchestration run.
- **Create a second CDC-specific transform DAG beside the existing batch DAG:**
  rejected. Both DAGs would write the same Gold tables and could overlap; two
  coordinators for one analytical graph add ambiguity without isolation.
- **Keep transform and publication in separate DAGs connected by
  `lakehouse://gold`:** rejected. Dataset ordering does not serialize a later
  transform against publication from an earlier DAG run; `max_active_runs` is
  per DAG.
- **Use an Airflow pool to serialize separate DAGs:** rejected as the primary
  contract. A pool can limit task concurrency but is weaker and less legible
  than expressing the required order in one DAG run.
- **Have the CDC consumer call Airflow or emit an Airflow Dataset event after
  each batch:** rejected. It couples the durable sink to scheduler availability,
  requires Airflow credentials in the streaming service, and can create a
  refresh storm. The consumer's delivery boundary must remain Kafka -> Iceberg.
- **Use a Kafka-trigger/deferrable-sensor provider:** rejected. It adds a new
  provider and runtime surface when an hourly timetable plus bounded polling is
  sufficient for the local workload.
- **Pause or stop the CDC consumer during dbt:** rejected. Orchestration must not
  control the long-running transport service, and pausing it would create lag
  and increase WAL/retention pressure. Offset predicates provide a stable read
  while ingestion continues.
- **Check lag zero but let dbt read all Bronze events:** rejected. The ledger can
  advance after the check, producing a moving model frontier.
- **Use source LSN alone as the orchestration boundary:** rejected. The sink's
  durable commit boundary is represented by Kafka consumer-group offsets, and
  Kafka offsets cannot be compared across topics. LSN remains downstream source
  ordering, not transport completion state.
- **Persist the last published boundary and skip unchanged hourly runs:**
  deferred. Airflow Variable/XCom lookup rules or an Iceberg control table would
  introduce another state protocol before resource measurements justify it.
- **Automate initial snapshot completion through JMX metrics now:** deferred to
  the observability phase. The existing paused-DAG bootstrap gate is explicit,
  bounded in frequency, and documented.
- **Make Airflow invoke Docker Compose or mount the Docker socket:** rejected for
  security and separation of responsibilities. Profile lifecycle remains an
  operator concern.
- **Incremental dbt or ClickHouse publication:** deferred. Current data volume
  supports deterministic full rebuilds, and ADR 0004's atomic full-snapshot swap
  already provides the required recovery behavior.

## Consequences

Positive:

- Phase 8 gains one observable Airflow path from stable CDC Bronze through dbt
  tests to atomic ClickHouse publication.
- Every automatic dbt run reads an immutable logical Kafka frontier even while
  the CDC consumer keeps appending to Bronze.
- dbt failure cannot trigger serving publication, and one DAG's
  `max_active_runs=1` prevents overlapping Airflow build/publication cycles.
- Existing batch Bronze updates still trigger analytical refreshes.
- No new service, provider, dependency, secret, or persistence table is added.
- Retry behavior remains structurally idempotent: Bronze transport coordinates,
  deterministic dbt models, and the ClickHouse staging swap tolerate reruns.

Negative and accepted costs:

- CDC freshness is bounded by the hourly schedule rather than event-by-event
  triggering.
- The boundary is transport quiescence, not a cross-topic atomic snapshot.
  Existing source-LSN modeling and fail-loud reconciliation remain necessary.
- The first automated run requires a documented operator bootstrap/unpause gate.
- Every hourly run rebuilds and republishes even when no CDC offset changed.
- The coordinated DAG now requires `streaming` and `bi` runtime profiles in
  addition to `core`; starting Airflow alone is insufficient for this DAG.
- Removing the standalone publication DAG changes the Airflow UI/task history
  shape, although the `transform_lakehouse` DAG ID is preserved.
- Manual dbt/publication commands can still bypass Airflow serialization, so the
  runbook must prohibit running them concurrently with an active refresh.

Operational signals are available immediately in task logs: connector/task
state, group activity, topic/partition, committed offset, high watermark, lag,
boundary attempts, elapsed time, dbt artifact summary, and publication summary.
Full platform metrics and alerts remain Phase 12 work.

## Rollback / migration considerations

Migration is coordinated but does not rewrite data:

1. Pause `transform_lakehouse` and `publish_serving`; wait for running/queued
   runs to finish.
2. Deploy the new Airflow image and DAG set: coordinated
   `transform_lakehouse`, no standalone `publish_serving`, and no `GOLD`
   orchestration Dataset event.
3. Start `core`, `streaming`, and `bi`; verify connector/consumer health and the
   initial-snapshot bootstrap gate if this is a clean CDC deployment.
4. Run DAG import/structure tests, then execute one controlled manual DAG run.
5. Reconcile Gold marts with ClickHouse and unpause the hourly/dataset schedule.

Rollback pauses the coordinated DAG and waits for its active run to finish,
then restores the previous `transform_lakehouse` -> `lakehouse://gold` ->
`publish_serving` DAG pair. The dbt boundary macro and optional runner argument
may remain because an omitted boundary preserves the former manual behavior.
No Kafka topic, consumer-group offset, CDC Bronze event, Iceberg Gold table, or
ClickHouse table must be deleted or migrated.

Rollback restores the previous operational limitation: automatic CDC refresh is
lost, dbt reads are not pinned by Airflow to a captured transport frontier, and
operators must again perform the runbook's stable-lag check before manually
building and publishing. It must not be presented as equivalent orchestration
correctness.
