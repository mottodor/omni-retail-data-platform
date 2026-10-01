# Runbook — PostgreSQL CDC (Debezium + Kafka -> Iceberg Bronze)

Scope: Phase 8 raw CDC for `customers`, `orders`, and `payments` (ADR 0006).
This is a local single-node deployment: no HA, TLS, or exactly-once claim.

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

Stop the consumer and Connect, record final offsets, then drop the inactive
slot/publication only if CDC is being disabled. Existing seven-table batch
snapshot ingestion remains operational and dbt still reads its snapshot-shaped
Bronze tables, so this slice can be rolled back without changing Silver/Gold.
