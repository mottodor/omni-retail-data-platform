# CDC Iceberg maintenance benchmark

## Purpose and boundary

This benchmark validates TD-006 against the delivered 210,000-row Debezium
clean replay. It measures Trino/Iceberg data-file compaction, raw-row
preservation, concurrent append behavior, retry/idempotency, consumer restart,
and the CDC-selected dbt graph on the target local workstation.

The result is evidence for this bounded workload only. It is not a claim that
the single-node stack or insert-only MERGE design scales beyond the measured
capstone fixture.

## Environment and reproducible workload

- Docker Engine 29.8.0 and Docker Compose 5.5.1 under WSL2;
- target budget: approximately 24 GiB RAM and 6 processors;
- repository Trino 483 image with the pinned REST-catalog patch;
- Polaris 1.7.0, Kafka 4.3.0, and Debezium 3.6.1.Final;
- disposable Compose project: `omni-retail-cdc-maintenance`;
- deterministic source: 10,000 customers, 100,000 orders, and 100,000
  payments;
- clean Kafka/Connect/Iceberg state before replay;
- `ICEBERG_CDC_FILE_SIZE_THRESHOLD_MB=128`;
- snapshot policy: 30 days and at least 10 recent `main` ancestors.

The initial replay reached stable lag zero with no Trino or consumer restart.
The first sample observed Trino at 5.651 GiB of its finite 6 GiB container limit
and the consumer at 81.72 MiB of 512 MiB.

## Initial shape

| Check | Before maintenance |
| --- | ---: |
| Rows | 210,000 |
| Distinct `event_id` | 210,000 |
| Snapshots | 421 |
| Current data files | 420 |
| Current file bytes | 26,754,310 |
| Smallest / largest file | 60,904 / 74,664 bytes |
| Trino restarts | 0 |
| CDC consumer restarts | 0 |
| Consumer lag | 0 on all three topics |

All 420 current files were below the 128 MiB threshold and belonged to one
`event_date` partition.

## First apply result

The confirmed host apply took 57.30 seconds end to end. The compaction stage
completed in approximately 46.5 seconds; the remaining time was inventory,
full-row preservation, uniqueness, and snapshot-policy checks.

| Check | Result |
| --- | ---: |
| Rewritten data files | 420 |
| Added data files | 1 |
| Removed delete files | 0 |
| Current files after compaction | 1 |
| Current file bytes after compaction | 25,412,188 |
| Rows / distinct `event_id` after | 210,000 / 210,000 |
| Missing or changed pre-snapshot rows | 0 |
| Trino / consumer restarts | 0 / 0 |

The semantic postcondition compares all 17 stored columns from the captured
pre-maintenance snapshot with current rows by `EXCEPT`; it permits additions
but fails for any missing or changed old row. It also requires current
`count(*) = count(DISTINCT event_id)`.

The fresh fixture had no snapshot older than 30 days, so expiration correctly
reported `no_op`; compaction added one replacement snapshot. The pinned-Trino
disposable-schema integration test separately uses Trino's test-only zero
retention session override and proves that `main` plus two rollback snapshots
remain readable. Runtime configuration retains the application and catalog
seven-day floor.

## Concurrent append and idempotency evidence

A deterministic 600-event source mutation produced 1,055 new CDC records and
four current files. A second maintenance run rewrote those four files into one.
While that run was active, a deterministic 20-event mutation committed 38
additional CDC records. The expiration-stage postcondition observed the
allowed growth from 211,055 to 211,093 rows and still found:

- zero missing or changed pre-existing rows;
- 211,093 distinct event IDs for 211,093 rows;
- lag zero at offsets customers 10,040, orders 100,531, payments 100,522;
- no Trino or consumer restart.

A during-run `docker stats` sample observed Trino at 5.692 GiB / 6 GiB and the
consumer at 80.46 MiB / 512 MiB. The new append left a second small file. The
next apply rewrote two files into one; an immediate repeated apply reported:

```text
status=no_op
rewritten_data_files_count=0
added_data_files_count=0
files_before=1
files_after=1
rows=211093
distinct_event_ids=211093
```

This demonstrates convergence after a partial sequence: compaction, a
concurrent append, follow-up compaction, and a retry/no-op.

## Restart and downstream result

A preserved-volume `streaming-down` / `streaming-up` cycle recovered the exact
committed offsets above with lag zero and retained 211,093 unique Bronze rows.
The replacement Kafka, Connect, and consumer containers did not reset
transport or Iceberg state.

After maintenance and restart, the selected CDC dbt graph completed 106/106
models/tests successfully in 11.45 seconds. The graph covered the three typed
CDC staging views, current-state views, customer versions, `dim_customer`,
`fact_orders`, and `fact_payments`, including order/payment reconciliation and
Gold key-set tests.

Final physical state:

| Check | Final result |
| --- | ---: |
| Rows / distinct `event_id` | 211,093 / 211,093 |
| Current data files | 1 |
| Current file bytes | 25,679,788 |
| Snapshots | 428 (fresh history; no snapshot age-eligible) |
| Trino / consumer restarts | 0 / 0 |
| Sampled Trino memory after validation | 5.323 GiB / 6 GiB |
| Sampled consumer memory after validation | 31 MiB / 512 MiB |

## Reproduction

Use a disposable project because clean replay creates new source, Kafka,
catalog, and object-storage volumes. Stop any project already using the fixed
loopback ports first.

```bash
COMPOSE_PROJECT_NAME=omni-retail-cdc-maintenance make up
COMPOSE_PROJECT_NAME=omni-retail-cdc-maintenance make generate-oltp
COMPOSE_PROJECT_NAME=omni-retail-cdc-maintenance make streaming-up
COMPOSE_PROJECT_NAME=omni-retail-cdc-maintenance make streaming-status
# Poll the typed status command until every topic reports lag 0.

COMPOSE_PROJECT_NAME=omni-retail-cdc-maintenance \
  make iceberg-cdc-maintenance-plan
COMPOSE_PROJECT_NAME=omni-retail-cdc-maintenance \
  make iceberg-cdc-maintenance-apply
COMPOSE_PROJECT_NAME=omni-retail-cdc-maintenance make streaming-status
```

Inspect identity and physical shape:

```bash
COMPOSE_PROJECT_NAME=omni-retail-cdc-maintenance docker compose exec trino \
  trino --output-format TSV --execute \
  'SELECT count(*), count(DISTINCT event_id)
   FROM iceberg.bronze.postgres_cdc_events;
   SELECT count(*)
   FROM iceberg.bronze."postgres_cdc_events$snapshots";
   SELECT count(*), sum(record_count), sum(file_size_in_bytes),
          min(file_size_in_bytes), max(file_size_in_bytes)
   FROM iceberg.bronze."postgres_cdc_events$files";'
```

Exercise post-replay appends, repeat maintenance, and offset recovery:

```bash
COMPOSE_PROJECT_NAME=omni-retail-cdc-maintenance make mutate-oltp EVENTS=600
COMPOSE_PROJECT_NAME=omni-retail-cdc-maintenance make streaming-status
COMPOSE_PROJECT_NAME=omni-retail-cdc-maintenance \
  make iceberg-cdc-maintenance-apply
COMPOSE_PROJECT_NAME=omni-retail-cdc-maintenance make mutate-oltp EVENTS=20
COMPOSE_PROJECT_NAME=omni-retail-cdc-maintenance make streaming-status
COMPOSE_PROJECT_NAME=omni-retail-cdc-maintenance \
  make iceberg-cdc-maintenance-apply
COMPOSE_PROJECT_NAME=omni-retail-cdc-maintenance \
  make iceberg-cdc-maintenance-apply  # expected no-op
COMPOSE_PROJECT_NAME=omni-retail-cdc-maintenance make streaming-down
COMPOSE_PROJECT_NAME=omni-retail-cdc-maintenance make streaming-up
COMPOSE_PROJECT_NAME=omni-retail-cdc-maintenance make streaming-status
```

Run the CDC-selected graph (the clean project intentionally has no batch/API
Bronze fixtures for the unrelated models):

```bash
COMPOSE_PROJECT_NAME=omni-retail-cdc-maintenance make dbt-build \
  ARGS="--indirect-selection cautious --select \
  stg_cdc_customers stg_cdc_orders stg_cdc_payments \
  int_cdc_customers_current int_cdc_orders_current \
  int_cdc_payments_current int_cdc_customer_versions \
  dim_customer fact_orders fact_payments"
```

Always remove all disposable-project volumes after the benchmark:

```bash
COMPOSE_PROJECT_NAME=omni-retail-cdc-maintenance docker compose \
  --profile core --profile streaming --profile bi --profile orchestration \
  down -v --remove-orphans
```
