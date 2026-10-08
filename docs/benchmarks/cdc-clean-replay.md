# CDC clean-replay resource benchmark

## Purpose

Bound the delivered 210,000-row Debezium initial snapshot on the target local
workstation and remove the Trino/consumer restart loop observed with the former
Trino memory configuration. This is maintenance of ADR 0006's existing
Kafka-to-Iceberg design; it does not change delivery identity, snapshot mode, or
offset-after-Iceberg semantics.

## Environment and workload

- Docker Engine 29.8.0 and Docker Compose 5.5.1 under WSL2;
- target budget: approximately 24 GiB RAM and 6 processors;
- Trino 483, Polaris 1.7.0, Kafka 4.3.0, Debezium 3.6.1.Final;
- fresh deterministic OLTP source: 10,000 customers, 100,000 orders, and
  100,000 payments;
- clean Kafka/Connect transport and absent
  `iceberg.bronze.postgres_cdc_events` before each accepted replay;
- CDC consumer group `omni-iceberg-bronze-cdc-v1`;
- measurements taken from consumer-group offsets, `docker stats`, container
  restart counts, Trino logs, and Iceberg metadata tables.

The lag-zero durations below start when sampling began after `streaming-up`
reported the consumer healthy. They are comparison evidence, not an SLA.

## Baseline and rejected alternatives

| Configuration | Result | Lag-zero sample time | Trino restarts | Consumer restarts |
| --- | --- | ---: | ---: | ---: |
| 2 GiB Trino heap, 500-event batch | Completed only through restart/replay | 584 s | 2 | 9 |
| 2 GiB Trino heap, 2,000-event batch | Rejected before replay | n/a | 0 | repeated failure |
| 2 GiB Trino heap, 1,000-event batch | Completed only through restart/replay | 706 s | 2 | 10 |

The 500-event baseline produced 421 snapshots and 420 data files for 210,000
unique rows. Trino terminated twice with `java.lang.OutOfMemoryError: Java heap
space`; the consumer replayed safely after each outage and eventually reached
lag zero.

Increasing the batch was not a safe fix:

- 2,000 events expanded through the Trino DB-API to a 3,181,094-character
  query, exceeding the configured 2,000,000-character limit;
- 1,000 events stayed below that limit but increased per-query planning
  pressure and still produced two Trino and ten consumer restarts;
- neither result justified relaxing the query-text ceiling or changing the
  delivery design.

The evidence therefore identified an undersized Trino heap for this bounded
workload, rather than Kafka polling throughput alone. The existing 500-event
batch remains the measured upper bound and is now rejected at configuration
validation when exceeded.

## Selected bounded fix

- keep `CDC_BATCH_SIZE=500`;
- increase Trino's JVM maximum heap from 2 GiB to 4 GiB;
- add a finite 6 GiB Trino container memory limit;
- keep `query.max-memory=1GB`, `query.max-total-memory=1536MB`, and
  `query.max-length=2000000` unchanged so the extra heap remains headroom for
  Iceberg/REST-catalog planning and metadata rather than an unbounded query
  allowance;
- classify Trino 483's transient `Failed to load view ...`
  `ICEBERG_CATALOG_ERROR` alongside its existing transient table-load variant,
  retaining the consumer's three-attempt bounded retry instead of restarting
  the process.

## Final result

The final clean replay reached lag zero in 465 seconds from the first sample:

| Check | Result |
| --- | ---: |
| Initial snapshot rows | 210,000 |
| Distinct initial `event_id` values | 210,000 |
| Trino process restarts | 0 |
| CDC consumer process restarts | 0 |
| Trino OOMs | 0 |
| Peak sampled Trino container memory | 5.72 GiB / 6 GiB |
| Initial Iceberg snapshots / data files | 421 / 420 |

A subsequent deterministic 20-event source mutation produced 37 CDC rows, all
with non-null source LSNs. The final ledger contained 210,037 rows and 210,037
distinct event IDs, with both Trino and the consumer still at zero restarts.
A preserved-volume `streaming-down` / `streaming-up` cycle retained the same
210,037 unique rows and returned healthy lag-zero status.

The selected change is intentionally bounded. It does not claim that the raw
MERGE design scales beyond the delivered workstation workload. The subsequent
[CDC maintenance benchmark](cdc-maintenance.md) measures small-file compaction,
snapshot policy, concurrent appends, and restart/idempotency at this same
clean-replay boundary.

## Reproduction outline

Use only a disposable Compose project because clean replay resets Kafka/Connect
state and the CDC Bronze table:

```bash
COMPOSE_PROJECT_NAME=omni-retail-startup-test make up
COMPOSE_PROJECT_NAME=omni-retail-startup-test make generate-oltp
COMPOSE_PROJECT_NAME=omni-retail-startup-test make streaming-up
COMPOSE_PROJECT_NAME=omni-retail-startup-test make streaming-status
```

Wait for lag zero, then inspect restart counts and Iceberg identity:

```bash
docker inspect -f '{{.Name}} restart={{.RestartCount}}' \
  omni-retail-startup-test-trino-1 \
  omni-retail-startup-test-cdc-consumer-1

COMPOSE_PROJECT_NAME=omni-retail-startup-test docker compose exec trino \
  trino --output-format TSV --execute \
  'select count(*), count(distinct event_id) from iceberg.bronze.postgres_cdc_events'
```

For a controlled post-snapshot check:

```bash
COMPOSE_PROJECT_NAME=omni-retail-startup-test make mutate-oltp EVENTS=20
COMPOSE_PROJECT_NAME=omni-retail-startup-test make streaming-status
```

Clean up only the disposable project with all delivered profiles enabled so its
named volumes are included in Compose project resolution:

```bash
COMPOSE_PROJECT_NAME=omni-retail-startup-test docker compose \
  --profile core --profile streaming --profile bi --profile orchestration \
  down -v --remove-orphans
```
