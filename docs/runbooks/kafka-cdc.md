# Runbook — PostgreSQL CDC (Debezium + Kafka -> Iceberg Bronze)

Scope: Phase 8 CDC for `customers`, `orders`, and `payments`: immutable Bronze delivery (ADR 0006) and CDC-authoritative Gold cutover (ADR 0007). This is a local single-node deployment: no HA, TLS, or exactly-once claim.

## Start and verify

Add unique values for `DEBEZIUM_POSTGRES_USER` and
`DEBEZIUM_POSTGRES_PASSWORD` to `.env`, then:

```bash
make up
make streaming-up
make streaming-status
```

`streaming-up` health-gates PostgreSQL, Trino, Kafka, Connect, and the consumer;
then idempotently reconciles the role/publication, topics, and connector. It
never relies on a fixed startup sleep.

Expected connector status shape:

```json
{"name":"omni-postgres-cdc","connector":{"state":"RUNNING"},"tasks":[{"state":"RUNNING"}]}
```

Inspect the raw ledger:

```sql
SELECT source_table, operation, count(*)
FROM iceberg.bronze.postgres_cdc_events
WHERE source_table IN ('customers', 'orders', 'payments')
GROUP BY 1, 2
ORDER BY 1, 2;
```

Initial startup uses `snapshot.mode=initial`: existing source rows appear as
`r`, then WAL changes as `c/u/d`. Snapshot completion is visible when the
connector remains `RUNNING`, source-topic offsets stop advancing without new
OLTP writes, and streaming mutations begin appearing with non-null LSNs.

## State and retention

Persistent state:

- `postgres-data`: OLTP data, publication, and logical slot
  `omni_cdc_slot`;
- `kafka-data`: source topics plus compacted Connect config/offset/status
  topics;
- Iceberg: `bronze.postgres_cdc_events` raw history.

Normal `make streaming-down && make streaming-up` preserves all three.
Data topics have one partition, delete retention of 7 days or 5 GiB per
partition. Connect internal topics are compacted with no time expiry.
PostgreSQL caps retained slot WAL at 2 GiB.

## Inspect connector, offsets, and lag

```bash
curl -fsS http://127.0.0.1:8083/connectors/omni-postgres-cdc/status | python -m json.tool
curl -fsS http://127.0.0.1:8083/connectors/omni-postgres-cdc/config | python -m json.tool

docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 \
  --group omni-iceberg-bronze-cdc-v1 --describe

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --describe \
  --topic omni.oltp.public.orders
```

Do not paste connector config into tickets or logs: the REST response includes
the PostgreSQL replication password.

## Delivery and replay semantics

The consumer disables automatic offset commits. For each bounded batch it:

1. validates topic routing and the Debezium envelope;
2. performs an insert-only Iceberg MERGE keyed by deterministic `event_id`;
3. synchronously commits each Kafka partition's next offset.

A crash after step 2 but before step 3 replays the same Kafka coordinates. The
MERGE finds the same event IDs and inserts nothing. A validation or Iceberg
write failure does not commit offsets. This is at-least-once transport with an
idempotent sink, not exactly-once processing.

For a bounded diagnostic consumer run from the host:

```bash
set -a; source .env; set +a
uv run python -m omni_retail.streaming.cdc run \
  --max-batches 1 --idle-timeout-seconds 30
```

Use a different `CDC_CONSUMER_GROUP_ID` for diagnostics unless intentionally
advancing the production-like group.

## Build and publish CDC-backed Gold

Gold is not automatically triggered by CDC. First wait for a stable consumed
boundary across all three data topics:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 \
  --group omni-iceberg-bronze-cdc-v1 --describe
```

Record each topic's `CURRENT-OFFSET`, `LOG-END-OFFSET`, and `LAG`. Proceed only
when every selected partition has lag 0 and two consecutive checks show the
same end offsets (or source writes are intentionally paused). Because Kafka
offers no cross-topic total order, a build during uneven order/payment lag can
fail reconciliation; wait for lag to clear and rerun rather than weakening the
test.

Run a coordinated full graph rebuild, then republish the unchanged mart schemas:

```bash
make dbt-build
make serving-publish
```

The build derives `dim_customer`, `fact_orders`, and `fact_payments` only from
CDC. Snapshot Silver stays available for rollback and supplies uncaptured
`order_items`/`shipments`; their Gold facts are filtered through the live CDC
order set. The ClickHouse full-snapshot swap removes rows that vanished from
Gold without a serving migration.

Inspect the raw history and the selected live winner together:

```sql
SELECT operation, source_lsn, kafka_offset, source_timestamp, ingested_at
FROM iceberg.bronze.postgres_cdc_events
WHERE source_table = 'orders'
  AND cast(json_extract_scalar(key_json, '$.order_id') AS bigint) = 123
ORDER BY source_lsn, kafka_offset;

SELECT order_id, status, source_lsn, kafka_offset, event_id
FROM iceberg.silver.int_cdc_orders_current
WHERE order_id = 123;
```

An initial `r` event is always the pre-streaming baseline, even when Debezium
supplies an LSN. The streamed winner is the greatest PostgreSQL LSN, then Kafka
offset within the fixed single-partition table topic. A latest delete correctly
returns no current-state row. A streamed `c/u/d` without LSN fails the dbt
contract. Do not diagnose state using `updated_at`, source or Kafka timestamp,
transaction ID, or `ingested_at`; those are audit fields.

If dbt fails during typed projection:

1. find the failing topic/partition/offset in dbt/Trino output;
2. inspect `key_json`, `after_json`, and `operation` in Bronze without editing
   the raw row;
3. classify the source change against `docs/data-contracts.md`;
4. for a missing required field or incompatible type, stop downstream work and
   update the source migration/contract/projection together;
5. rerun the selected dbt graph. Do not replace strict casts with `try_cast` or
   suppress a required-field test merely to make the build green.

Inspect customer SCD2 and live Gold state after the build:

```sql
SELECT customer_id, customer_key, is_snapshot_baseline,
       valid_from_lsn, valid_to_lsn, is_current
FROM iceberg.gold.dim_customer
ORDER BY customer_id, is_snapshot_baseline DESC, valid_from_lsn;

SELECT order_id, customer_id, customer_key
FROM iceberg.gold.fact_orders
ORDER BY order_id;
```

Deletes close customer history without emitting an attribute-less dimension
row and remove winning order/payment keys. A later customer create opens a new
lifecycle. An order `r` uses the baseline customer key; an order `c` uses the
half-open customer LSN interval containing its create LSN. Later order updates
do not re-key the fact. Missing/ambiguous customer history fails the build;
there is no fallback.

A new CDC order can temporarily have no line/shipment rows until the next
snapshot. This is expected. A deleted order must have no Gold line/shipment
rows even if stale rows remain in snapshot Silver.

## Source schema changes

Classify and review a source migration before applying it:

- adding a nullable or defaulted non-key column is compatible with the raw CDC
  ledger; Debezium places it in `before`/`after` JSON and no Bronze DDL is
  needed;
- changing a primary key, route, or required envelope semantics is
  incompatible and will fail-stop the consumer before offset commit;
- renaming, dropping, or changing the type of a non-key column can still enter
  raw JSON because the converter is schema-less, but it is breaking for typed
  downstream consumers and requires a contract/model impact review first.

For a compatible additive migration:

1. record connector status, consumer lag, and the slot's
   `confirmed_flush_lsn`;
2. apply the versioned PostgreSQL migration;
3. emit one controlled row change that populates the new field;
4. query `envelope_json`/`after_json` in Bronze and verify the field, value,
   unique `event_id`, running connector, and advancing slot/consumer offsets;
5. update typed consumers separately before they depend on the field.

Do not restart or reset the connector merely because the source relation
changed. Do not use ingestion timestamp as source order. Kafka order is only
per single-partition table topic; cross-topic order is not defined, and raw
records with older event time remain valid separate events.

The live integration test reserves the nullable test-only column
`public.customers.cdc_schema_evolution_note` and removes it in teardown. If a
killed test leaves it behind, first ensure no integration test is running, then
clean only that column:

```sql
ALTER TABLE public.customers
  DROP COLUMN IF EXISTS cdc_schema_evolution_note;
```

The next test run also repairs this exact reserved column while holding a
PostgreSQL advisory lock. Never use this cleanup pattern for a business column.

## Failure procedures

### Connector or task is FAILED

1. Record the status without publishing the config/password.
2. Inspect bounded logs:

   ```bash
   docker compose logs --tail=200 debezium-connect
   ```

3. Check PostgreSQL publication and slot:

   ```sql
   SELECT pubname FROM pg_publication WHERE pubname = 'omni_cdc_publication';
   SELECT slot_name, active, restart_lsn, confirmed_flush_lsn
   FROM pg_replication_slots WHERE slot_name = 'omni_cdc_slot';
   ```

4. Re-run `make streaming-up`; bootstraps are idempotent.
5. Do not drop the slot merely to clear an error. Use destructive reset only
   after accepting a new initial-snapshot boundary.

### Slot WAL exceeded 2 GiB / slot invalidated

The bound protects the workstation from unbounded disk growth. Recovery loses
the old replay boundary:

1. stop writes if a gap-free business recovery is required;
2. record source and Kafka offsets plus the last Bronze LSN;
3. run the explicit transport reset below;
4. let the new initial snapshot finish;
5. document the old/new snapshot boundary. Do not claim gap-free recovery
   without reconciliation against a source snapshot.

### Consumer unhealthy or lag grows

Check `docker compose logs --tail=200 cdc-consumer`, Trino health, and consumer
group offsets. Malformed records fail-stop with topic/partition/offset context;
they are not skipped. Phase 8 has no DLQ. Fix or explicitly reset/replay the
record only after identifying its contract impact.

### Kafka or Connect restart

```bash
docker compose restart kafka debezium-connect cdc-consumer
make streaming-status
```

Wait for healthchecks and connector/task `RUNNING`; apply a controlled source
mutation and verify a new Bronze row. Existing event IDs must remain unique.

## Destructive reset

```bash
make streaming-reset
```

This clearly destructive command removes Kafka records/Connect state and drops
the PostgreSQL slot/publication before recreating them. It preserves OLTP data,
core lakehouse volumes, and `bronze.postgres_cdc_events`.

Because the new initial snapshot receives new Kafka offsets, retaining Bronze
creates a second raw snapshot history with new event IDs. For a clean
end-to-end replay, separately and explicitly clear only the CDC table first:

```sql
DROP TABLE iceberg.bronze.postgres_cdc_events;
```

Then run `make streaming-reset`. Never hide the table drop inside the transport
reset: analytical history and transport state have separate ownership.

## Rollback

For a Gold-only rollback, do not delete CDC Bronze or stop capture. Revert
`dim_customer`, `fact_orders`, and `fact_payments` to their snapshot-backed
references, restore date-validity SCD2, run a full dbt rebuild, and republish
ClickHouse. Snapshot ingestion/Silver remain operational specifically for this
path. Record that rollback reintroduces the known correctness regression: hard
deletes survive as their last snapshot rows.

To disable CDC itself, first stop the consumer and Connect and record final
offsets. Drop the inactive slot/publication only after accepting that recovery
boundary. Do not remove the immutable Bronze ledger merely because Gold rolled
back.
