# Active plan — Phase 8 slice 4: CDC-backed Gold cutover

Task: switch the existing Kimball Gold path for `customers`, `orders`, and
`payments` from PostgreSQL snapshots to the immutable CDC ledger, while keeping
snapshot-only `order_items` and `shipments` referentially safe. This is the
single execution/resume artifact defined by AGENTS §41.3; roadmap acceptance
criteria remain in `ROADMAP.md`, architecture in ADRs, and status in
`PROGRESS.md`. Delete or replace this file in the implementation-closing
commit.

Created: 2026-10-01 (planning session).

## Goal

A full `dbt build` must derive delete-aware Gold dimensions/facts and unchanged
mart schemas from `bronze.postgres_cdc_events`:

- customer updates, deletes, and recreates produce deterministic SCD2 history;
- the latest order/payment delete removes that business key from Gold;
- snapshot-only order items and shipments never survive without a live CDC
  parent order;
- a CDC replay or a repeated dbt build produces identical Gold and mart rows;
- the existing ClickHouse full-snapshot publisher can propagate those removals
  without a serving schema migration.

The slice is complete when deterministic integration fixtures prove initial
snapshot, update, delete, recreate, stale-child suppression, same-LSN handling,
and repeat-build behavior. Automatic CDC-triggered dbt/serving scheduling is a
separate orchestration slice.

## Context to read first

1. `AGENTS.md`; `docs/agent/streaming.md`, `dbt-modeling.md`, `lakehouse.md`,
   `reliability.md`, `testing-perf.md`, `airflow.md`, `serving-bi.md`, and
   `engineering-practices.md`.
2. `PROGRESS.md`; `ROADMAP.md` Phase 8 and Gate E.
3. `docs/adr/0006-cdc-deployment-and-delivery.md`, accepted cutover decision
   `docs/adr/0007-cdc-gold-cutover.md`, and `docs/adr/README.md`.
4. Contracts and current semantics in `docs/data-model.md`,
   `docs/data-contracts.md`, and `docs/runbooks/kafka-cdc.md`.
5. CDC models under `dbt/models/staging/` and `dbt/models/intermediate/`, then
   `dbt/models/core/`, `dbt/models/marts/`, and `dbt/tests/`.
6. Deterministic fixtures and assertions in
   `tests/integration/lakehouse_seed.py`, `test_dbt_core_build.py`,
   `test_lakehouse_orchestration.py`, and `test_postgres_cdc.py`.
7. Publication behavior in ADR 0004 and
   `tests/integration/test_serving_publication.py` (full snapshot swap means no
   ClickHouse DDL should be needed if mart columns stay stable).
8. Upstream references checked during planning through Context7:
   Debezium 3.6 (`/websites/debezium_io_reference_3_6`) for PostgreSQL source
   LSN/transaction/snapshot metadata and dbt docs
   (`/dbt-labs/docs.getdbt.com`) for half-open, non-overlapping SCD2 validity.

## Decisions already made

- CDC becomes authoritative in Gold for the three captured entities. Do not
  union CDC current state with snapshot-backed `int_customers`, `int_orders`,
  or `int_payments`: that would resurrect hard-deleted rows. Keep the snapshot
  models and ingestion operational as rollback inputs and for uncaptured
  tables, but remove them from these Gold dependencies.
- `fact_orders` reads `int_cdc_orders_current`; `fact_payments` reads
  `int_cdc_payments_current`. Their business grain and published columns stay
  unchanged.
- `dim_customer` is rebuilt deterministically from typed CDC event history,
  not from mutable dbt snapshot state. A delete closes the preceding version
  and emits no attribute-less dimension row; a later create starts a new
  lifecycle. `customer_key` must include the selected event identity rather
  than a date, so same-day versions and delete/recreate cycles cannot collide.
- Source succession/current/delete selection continues to use the established
  authoritative tuple: PostgreSQL LSN (snapshot baseline first), then the
  single-partition customer-topic offset, then `event_id`. Event time,
  `updated_at`, and ingestion time must not select the current row.
- ADR 0007 ratifies transaction-boundary customer history with half-open
  PostgreSQL source-LSN intervals: collapse multiple customer events at one
  LSN to the final customer-topic offset, treat `r` as the pre-streaming
  baseline, use the lifecycle-creating order event's source LSN for streamed
  orders, and use the baseline customer version for snapshot `r` orders. Later
  order updates must not re-key the fact. Kafka offsets are never
  compared across topics. An unmatched customer version fails loudly; the old
  batch-date validity and earliest/current-version fallbacks are retired.
- `fact_order_items` and `fact_shipments` remain snapshot-backed because those
  tables are not captured by Debezium. Both must inner/semi-join the live
  `fact_orders` key set, so PostgreSQL cascade deletes cannot leave stale Gold
  children. New-order children are eventually complete only after the next
  snapshot; do not hide that hybrid consistency window or require every live
  order to have child rows at every CDC instant.
- `dim_date` derives its bounds from CDC-backed `fact_orders`, not stale
  `int_orders`. Marts retain their current columns and business formulas.
  `mart_daily_sales` follows available live order lines;
  `mart_customer_ltv` follows live CDC orders and the current live customer
  version.
- Cross-topic order/payment arrival can be temporarily partial. Keep the
  existing fail-loud orders/payments reconciliation test; run Gold only at a
  stable consumed boundary and rerun after lag clears rather than weakening
  the contract.
- No new service, dependency, source table, Kafka topic, ClickHouse migration,
  Superset metadata change, or automatic Airflow trigger belongs to this
  slice.

## Steps

- [x] 1. Added and accepted ADR 0007
  (`docs/adr/0007-cdc-gold-cutover.md`) and indexed it. It ratifies
  CDC-vs-snapshot ownership, source-sequence SCD2 boundaries, point-in-time
  customer-key selection, parent-gated child facts, hybrid consistency,
  rollback, same-LSN/null-LSN behavior, and the distinction between PostgreSQL
  source order and cross-topic Kafka order.
- [ ] 2. Extend deterministic CDC fixtures before changing production SQL:
  - seed CDC `r` baselines for snapshot customer/order/payment IDs so the
    cutover has a complete initial state;
  - add customer update, delete, recreate, same-LSN, and regressing-event-time
    cases, plus an order update that must retain its creation-time customer key;
  - delete an order/payment whose snapshot-only item/shipment rows remain in
    Bronze, and add a live CDC order whose children have not been snapshotted;
  - keep every fixture timestamp/LSN/offset explicit and replay one event ID.
- [ ] 3. Add a customer-history intermediate model (expected name:
  `int_cdc_customer_versions`) that:
  - orders every event before applying delete semantics;
  - normalizes the initial `r` baseline and collapses same-transaction events
    according to ADR 0007;
  - emits one row per non-delete SCD2 version with deterministic
    `customer_key`, validity boundaries, current flag, and audit coordinates;
  - closes a version on delete, leaves deleted keys with zero current rows,
    and allows a later recreate to become current.
  Document/test the model in `dbt/models/intermediate/cdc_schema.yml`.
- [ ] 4. Cut over the core graph:
  - `dim_customer` selects the new CDC version model;
  - `fact_orders` selects `int_cdc_orders_current` and resolves the matching
    customer version under ADR 0007;
  - `fact_payments` selects `int_cdc_payments_current`;
  - `fact_order_items` and `fact_shipments` retain snapshot attributes but
    filter through `fact_orders`;
  - `dim_date` derives bounds from `fact_orders`.
  Preserve outward fact/mart schemas. Apply ADR 0007's intentional
  `dim_customer` contract change from daily date validity to explicit LSN
  validity plus audit-only source timestamps.
- [ ] 5. Update dbt contracts and business tests:
  - replace daily batch-date SCD2 descriptions with the chosen source-sequence
    semantics;
  - prove non-overlapping half-open intervals, at most one current row per
    source customer, zero current rows after delete, and one after recreate;
  - prove every child/payment fact references a live order and every fact order
    references an emitted customer version;
  - retain status, amount, temporal, line arithmetic, and order/payment
    reconciliation tests without downgrading failures.
- [ ] 6. Strengthen live integration assertions:
  - `test_dbt_core_build.py`: Gold uses CDC updates, removes deleted
    orders/payments/customers, suppresses stale snapshot-only children, keeps
    a new parent while child snapshots lag, and is identical on a second build;
  - `test_lakehouse_orchestration.py`: the existing full dbt runner still
    builds marts from the cut-over graph;
  - `test_postgres_cdc.py`: extend the selected dbt graph only as far as is
    safe against the live Bronze fixture, proving at least source mutation ->
    CDC -> delete-aware core state without mutating shared production schemas.
- [ ] 7. Verify downstream behavior without broadening implementation:
  - assert mart results exclude deleted orders and stale lines;
  - run the existing Gold-to-ClickHouse publication/reconciliation test and
    republish test; full-snapshot swap must remove vanished mart rows;
  - do not alter ClickHouse/Superset schemas when the contracts are unchanged.
- [ ] 8. Update `README.md`, `docs/data-model.md`,
  `docs/data-contracts.md`, and `docs/runbooks/kafka-cdc.md` with authoritative
  lineage, SCD2 delete/recreate semantics, hybrid child lag, stable-boundary
  build procedure, rollback to snapshot-backed models, and the remaining lack
  of automatic CDC -> dbt -> serving triggering.
- [ ] 9. Close the slice: run the validation below, update `PROGRESS.md` with
  the exact completed boundary and next Phase 8 gap, record only lasting
  follow-ups, and delete/replace this plan in the same commit.

## Validation

Run and report only commands actually executed successfully:

```bash
make lint
make test
docker compose config
make dbt-parse
make dbt-compile
make up
make dbt-build
OMNI_INTEGRATION=1 uv run pytest tests/integration/test_dbt_core_build.py -v
OMNI_INTEGRATION=1 uv run pytest tests/integration/test_lakehouse_orchestration.py -v
OMNI_INTEGRATION=1 uv run pytest tests/integration/test_postgres_cdc.py -v
OMNI_INTEGRATION=1 uv run pytest tests/integration/test_serving_publication.py -v
make integration
```

If `make dbt-parse` or `make dbt-compile` is not a repository target, run the
corresponding locked CLI command with `uv run dbt ... --project-dir dbt
--profiles-dir dbt` and record the substitution; do not add a Make target only
for this plan.

Manual acceptance queries should compare, for fixture-owned IDs:

```sql
SELECT customer_id, customer_key, is_current, valid_from_lsn, valid_to_lsn
FROM iceberg.gold.dim_customer
ORDER BY customer_id, valid_from_lsn;

SELECT order_id FROM iceberg.gold.fact_orders ORDER BY order_id;
SELECT order_id FROM iceberg.gold.fact_order_items ORDER BY order_id;
SELECT order_id FROM iceberg.gold.fact_shipments ORDER BY order_id;
```

Use the final ADR column names if they differ; the assertions, not these draft
names, are normative.

## Scope fence

In scope: ADR for mixed CDC/snapshot Gold ownership, customer SCD2 delete and
recreate semantics, CDC-backed order/payment facts, stale snapshot-child
suppression, unchanged marts/serving verification, tests, and documentation.

Out of scope:

- capturing `order_items`, `shipments`, products, or categories with Debezium;
- disabling PostgreSQL snapshot ingestion or deleting snapshot Silver models;
- automatic CDC dataset events, Airflow scheduling, continuous dbt builds, or
  automatic ClickHouse publication;
- changing dashboard definitions or business KPI formulas;
- Schema Registry, DLQ, observability, compaction/retention, Spark, or
  exactly-once claims;
- open infrastructure/file-ingestion issues #17 and #18.
