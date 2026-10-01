# ADR 0007 — CDC-backed Gold cutover and mixed child semantics

## Title

Make PostgreSQL CDC authoritative for customer, order, and payment Gold models;
model customer SCD2 validity by PostgreSQL source sequence; and gate
snapshot-only order items and shipments by the live CDC order set.

## Status

Accepted.

## Context

Phase 8 already delivers an immutable Debezium event ledger in
`iceberg.bronze.postgres_cdc_events` and typed, delete-aware current-state dbt
models for `customers`, `orders`, and `payments` (ADR 0006). Gold still reads
the older PostgreSQL snapshot path:

- `dim_customer` derives daily SCD2 versions from snapshot batch dates;
- `fact_orders` and `fact_payments` read snapshot-backed current-state views;
- `fact_order_items` and `fact_shipments` also read snapshots.

That split prevents the CDC slice from changing business outputs. A hard delete
is preserved in Bronze and removed from CDC Silver current state, but the last
snapshot version remains in Gold, marts, ClickHouse, and Superset.

The cutover is constrained by these facts:

- Debezium's initial `r` events provide a complete baseline for all three
  captured tables. Unioning that baseline with snapshot Silver would duplicate
  live entities and, more seriously, could resurrect a key whose latest CDC
  event is a delete.
- PostgreSQL source LSN is the established source-order boundary. A table
  topic has one Kafka partition, so its offset is a deterministic tie-break
  for events of one entity. Event time, source-row `updated_at`, Kafka time,
  and ingestion time can move backwards and cannot select current state.
- An initial Debezium snapshot is a pre-streaming baseline. Its `r` events can
  have nullable or non-comparable LSN details and must not outrank later
  streaming changes merely because of timestamp or transport metadata.
- Several row changes can share one PostgreSQL transaction LSN. Gold needs the
  transaction-boundary state, not artificial intermediate SCD2 versions inside
  one committed transaction.
- Kafka provides no ordering across the three table topics. PostgreSQL LSN can
  be used as a source transaction boundary for records emitted by this one
  connector/database, but Kafka offsets must never be compared across topics.
- `order_items` and `shipments` are not in the Phase 8 Debezium publication.
  PostgreSQL deletes both through `ON DELETE CASCADE` when an order is deleted,
  but snapshot extraction cannot observe those deletes. Conversely, children
  of a newly created order are not visible analytically until the next
  snapshot.
- Orders and payments are separate topics. A dbt run taken while consumer lag
  differs between topics can observe a temporarily incomplete transaction.
- ClickHouse publication is already a full-snapshot staging swap (ADR 0004).
  If mart schemas stay stable, a normal republish can remove rows that vanish
  from Iceberg without a ClickHouse migration.

The cutover therefore needs one explicit ownership rule, deterministic SCD2
and point-in-time foreign-key semantics, an honest policy for uncaptured child
tables, and a reversible migration path.

## Decision

### Gold ownership

CDC is the only Gold input for the three captured entities:

- `dim_customer` derives from typed customer CDC history;
- `fact_orders` derives from `int_cdc_orders_current`;
- `fact_payments` derives from `int_cdc_payments_current`.

The snapshot-backed `int_customers`, `int_orders`, and `int_payments` models
remain operational as rollback inputs, but Gold does not union or fall back to
them. Missing CDC state must fail visibly rather than silently reintroducing a
stale snapshot row.

Gold remains a current-state dimensional model for orders and payments. A
winning `d` event removes the corresponding row from the fact table. Raw
create/update/delete history remains in the immutable Bronze ledger; Gold does
not add tombstone fact rows or an `is_deleted` flag.

### Authoritative source sequence

For each business key, dbt constructs transaction-boundary state in this
order:

1. `op = 'r'` is the pre-streaming baseline, regardless of whether its source
   LSN is null or populated.
2. Streaming `c/u/d` events are ordered by `source_lsn` ascending.
3. Events for the same business key and LSN collapse to the final event by the
   entity topic's Kafka offset, with `event_id` as the deterministic final
   tie-break.

A streaming `c/u/d` event without `source_lsn` is incompatible with this Gold
contract and fails a dbt quality check. Source timestamp, row `updated_at`,
Kafka timestamp, transaction ID, and ingestion time remain audit fields only;
they never override source sequence.

This decision does not introduce a cross-topic Kafka ordering claim. Kafka
offsets are compared only inside one fixed single-partition table topic.
Where customer and order state must be related, their PostgreSQL LSNs are
compared as source transaction boundaries from the same connector/database.
Events with the same LSN are treated as one committed source boundary, not as
an ordered sequence across Kafka topics.

### Customer SCD Type 2

`dim_customer` is rebuilt deterministically from the full typed customer event
history. It does not use mutable dbt snapshot state.

Each transaction-boundary `r/c/u` winner emits one dimension version. A `d`
winner emits no attribute-less dimension row; instead, it closes the preceding
non-delete version. A later `c` for the same `customer_id` starts a new
lifecycle. Thus:

- a live customer has exactly one current version;
- a deleted customer has historical versions but zero current versions;
- a recreated customer has the old closed lifecycle plus one current version.

The surrogate key is:

```text
<customer_id>_<event_id>
```

The selected event identity makes the key deterministic and prevents
collisions between same-day changes or delete/recreate lifecycles.

The old daily `valid_from`/`valid_to` date columns are replaced by explicit
source-sequence boundaries:

- `valid_from_lsn` — inclusive streaming boundary; null only for an initial
  snapshot baseline;
- `valid_to_lsn` — exclusive boundary from the next transaction-level customer
  change or delete; null only for a live open version;
- `is_snapshot_baseline` — distinguishes the pre-streaming version from a
  streamed version;
- `is_current` — true only for the one open live version.

Source timestamps for opening and closing events may be exposed as audit
columns, but they are not validity boundaries and carry no non-overlap or
monotonicity claim. SCD2 tests operate on the baseline marker and half-open LSN
intervals, not wall-clock values.

Multiple customer changes at the same LSN collapse before intervals are built.
If the final same-LSN operation is `d`, the prior version closes at that LSN.
If it is `c` or `u`, only the final committed attribute state becomes a
version.

### Order-to-customer key resolution

`fact_orders` takes current mutable order attributes from the winning CDC
state, but freezes `customer_key` at the start of the current order lifecycle:

- a lifecycle beginning with `r` uses the customer's initial snapshot baseline
  version;
- a lifecycle beginning with `c` uses the customer version whose half-open
  PostgreSQL LSN interval contains that create event's `source_lsn`;
- later order `u` events change fact attributes but do not re-key the order to
  a newer customer version;
- after an order delete/recreate cycle, the new `c` event starts a new lifecycle
  and establishes the replacement fact's customer key;
- a customer version beginning at the same LSN as the order creation is
  eligible, so a same-transaction order observes the committed
  after-transaction customer state.

This resolution does not compare Kafka offsets across topics. There is no
earliest-version or current-version fallback. A live order without exactly one
matching lifecycle-start event and customer version receives no valid foreign
key and fails dbt `not_null`, relationship, and dedicated cardinality tests.
This makes a missing baseline, consumer gap, incompatible LSN, or invalid
source relationship visible rather than silently restating history.

### Snapshot-only children

`fact_order_items` and `fact_shipments` continue to obtain their attributes
from snapshot-backed Silver because those tables are outside the Debezium
publication. Each model inner- or semi-joins the live CDC-backed
`fact_orders` key set.

This parent gate defines the hybrid behavior:

- stale snapshot children disappear from Gold as soon as their parent order's
  winning CDC event is a delete;
- no child fact can reference a non-live Gold order;
- a newly created live order can temporarily have no item or shipment facts
  until the next snapshot captures them;
- tests enforce no orphan children, but do not require every live order to have
  snapshot children at every CDC instant.

`dim_date` derives its activity bounds from CDC-backed `fact_orders`, not from
stale snapshot `int_orders`.

### Quality boundary and downstream publication

The existing orders/payments business reconciliation remains fail-loud. dbt
Gold builds run only after the consumer has reached a stable boundary across
the selected topics. A partial cross-topic view is retried after lag clears;
the reconciliation is not weakened and no grace-period state is published.

The mart column contracts and KPI formulas remain unchanged:

- `mart_customer_ltv` follows live CDC orders and the current live customer
  version;
- `mart_daily_sales` follows live orders and currently available parent-gated
  item snapshots, so its documented child-snapshot lag applies;
- unrelated marketing and delivery-API marts are unchanged.

ClickHouse and Superset schemas do not change. The existing ClickHouse
full-snapshot staging swap republishes the rebuilt marts and removes vanished
rows atomically.

This ADR does not add an automatic CDC -> dbt -> ClickHouse trigger. The
cutover is invoked through the existing full dbt build and serving publication
entry points at a stable consumer boundary. Continuous scheduling is a
separate orchestration decision.

## Alternatives considered

- **Union CDC and snapshot current state:** rejected. It duplicates the initial
  baseline and can resurrect rows whose winning CDC event is a delete.
- **Keep snapshot Gold and use CDC only for audit:** rejected. Deletes would
  remain invisible to business outputs and Phase 8 would stop before the
  required current-state behavior.
- **Use source timestamp, row `updated_at`, ingestion time, or daily batch date
  for SCD2 validity:** rejected. Those clocks can regress or collapse distinct
  changes and are not the authoritative state-order contract.
- **Preserve every same-LSN customer event as a version:** rejected. Same-LSN
  events belong to one committed source boundary; Kafka offset is used only to
  select its final entity state.
- **Compare Kafka offsets between customer and order topics:** rejected.
  Offsets have meaning only within their own topic-partition.
- **Use the current or earliest customer version when no point-in-time match
  exists:** rejected. It would conceal source/consumer gaps and silently assign
  incorrect historical attributes.
- **Re-key an existing order on every order update:** rejected. Mutable order
  status changes must not restate the customer dimension version that was
  selected when the current order lifecycle began.
- **Represent deletes as Gold tombstone rows or `is_deleted` facts:** rejected.
  Gold is current business state; immutable delete history already exists in
  Bronze.
- **Retain the old date validity columns beside LSN validity:** rejected. Two
  competing validity systems would invite incorrect joins; source timestamps
  remain explicitly audit-only.
- **Hide a new order until item snapshots arrive:** rejected. It would make
  captured order/payment current state depend on an unrelated daily snapshot
  and add avoidable latency.
- **Allow stale child facts until the next snapshot:** rejected. It violates
  referential integrity immediately after a cascade delete.
- **Expand Debezium to order items and shipments in the same slice:** deferred.
  It changes the publication, event volume, typed models, and recovery surface;
  the parent gate is the smallest correct behavior for the accepted three-table
  CDC scope.
- **Downgrade transient order/payment reconciliation failures to warnings:**
  rejected. Publishing a partial business transaction would hide an observable
  consistency failure.
- **Add automatic CDC-triggered dbt and serving runs now:** deferred. Correct
  stable-boundary detection and orchestration deserve a separate bounded task.

## Consequences

Positive:

- hard-deleted customers, orders, and payments no longer survive in Gold or
  derived serving marts;
- customer update/delete/recreate history is deterministic and rebuildable
  from the immutable ledger;
- regressing timestamps cannot corrupt current-state or SCD2 succession;
- stale order items and shipments cannot violate Gold parent relationships;
- repeated dbt builds and CDC transport replay produce the same business state;
- ClickHouse continues to be a schema-compatible derived layer.

Negative and accepted costs:

- `dim_customer` changes from business-date validity to PostgreSQL
  source-sequence validity. Ad-hoc consumers of the old `valid_from` and
  `valid_to` columns must migrate to the explicit LSN contract.
- Existing `customer_key` values change during the full cutover rebuild because
  keys now contain event identity. All dependent facts and marts must rebuild
  together.
- LSN validity is PostgreSQL/Debezium-specific and describes system/source
  sequence, not a wall-clock business-effective interval.
- Exact cross-table state is available only when both events have compatible
  source LSNs from this connector. Missing streamed LSNs or unmatched customer
  versions fail the build.
- Snapshot-only item and shipment facts are intentionally eventually complete
  for new orders. `mart_daily_sales` can lag order/LTV views until the next
  child snapshot.
- A dbt run started before all relevant topics reach a stable boundary can fail
  reconciliation and must be rerun after lag clears.
- The slice does not continuously refresh Gold or serving data merely because
  CDC Bronze advanced.

Operationally, the cutover is a coordinated full rebuild, not an incremental
in-place schema evolution: build Silver/Gold/marts together, run all dbt tests,
then republish ClickHouse through the existing atomic swap.

## Rollback / migration considerations

Migration to the CDC-backed graph:

1. Verify the connector and consumer are healthy and record per-topic lag.
2. Wait for a stable consumed boundary across customer, order, and payment
   topics.
3. Run a full dbt build in an isolated schema and compare live/deleted fixture
   IDs, row counts, reconciliation, SCD2 intervals, and parent/child integrity
   with the snapshot-backed baseline.
4. Rebuild the normal Gold and mart schemas as one graph. Do not reuse old
   `customer_key` values.
5. Publish the unchanged mart schemas to ClickHouse with the existing
   full-snapshot swap and run Gold-versus-serving reconciliation.

Rollback does not delete CDC data. Revert the Gold model references and
`dim_customer` implementation to the snapshot-backed versions, run a full dbt
Gold/mart rebuild, and republish ClickHouse. The snapshot Bronze/Silver path
remains operational specifically to make this possible.

Rollback restores the known snapshot limitation: hard deletes again remain as
their last observed rows. It must therefore be recorded as a temporary
correctness regression, not presented as equivalent behavior.

Future expansion of Debezium to `order_items` or `shipments` replaces the
corresponding snapshot input and parent-gate lag; it does not require changing
CDC ownership of customers, orders, or payments. Future automatic orchestration
must preserve the stable-boundary and fail-loud reconciliation contract defined
here.
