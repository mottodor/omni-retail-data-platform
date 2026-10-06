# Data Contracts

- **Status:** maintained baseline for the delivered interfaces; ownership and
  freshness values are educational placeholders, not production SLAs
- **Scope:** external batch sources plus the delivered PostgreSQL raw CDC
  ledger, typed Silver projections, and CDC-backed customer/order/payment Gold
  models
- **Enforcement:** schema definitions in
  `omni_retail.ingestion.files.schemas`, API envelope checks, fail-stop
  Debezium envelope/route validation in `omni_retail.streaming.cdc`, and dbt
  typed/current/Gold tests; file violations quarantine data, malformed CDC
  records block offset advancement, and incompatible typed or Gold CDC state
  fails the dbt build

Contract coverage is intentionally limited to the delivered interfaces listed
here; this repository makes no commitment to a broader future contract layer.

## Conventions

- All identifiers are lower-case snake_case.
- Timestamps are UTC, ISO-8601. Dates are ISO-8601 calendar dates.
- Monetary values are decimals with 2 fractional digits; rates are floats as published by the source.
- The raw payload stored in the `archive` bucket is immutable and never rewritten by the platform.

---

## File sources

All file sources share the same delivery mechanics:

| Aspect | Value |
|---|---|
| Drop zone | `s3://landing/<source>/incoming/<filename>` |
| Transit | `s3://landing/<source>/processing/<filename>` (interrupted-run marker) |
| Raw archive | `s3://archive/<source>/<yyyy>/<mm>/<dd>/<filename>` (unchanged payload) |
| Quarantine | `s3://rejected/<source>/<yyyy>/<mm>/<dd>/…` (+ `.rejection.json`, `.badrows.<ext>`) |
| Batch identity | `batch_id = <source>-<sha256[:16]>` (content-addressed) |
| Duplicate policy | same content ⇒ status `duplicate`, skipped |
| Metadata | `archive/_manifests/<source>/<batch_id>.json` (checksum, row counts, schema version, reasons) |

### supplier-prices (CSV)

| Field | Type | Required | Notes |
|---|---|---|---|
| supplier_id | string | yes | supplier slug |
| sku | string | yes | supplier-local SKU |
| price | decimal ≥ 0 | yes | 2 fractional digits |
| currency | string | yes | ISO 4217 (EUR/USD/GBP) |
| valid_from | date | yes | price validity start |

- **Grain:** one row per (supplier_id, sku, valid_from).
- **Compatibility policy:** additive columns are backwards-incompatible for this pipeline (unexpected column ⇒ whole file rejected) and require a new `schema_version` plus validator update; column removal/rename is a breaking change.
- **Freshness (draft):** daily by 06:00 UTC for the current day.

### partner-products (JSON)

| Field | Type | Required | Notes |
|---|---|---|---|
| partner_id | string | yes | partner slug |
| sku | string | yes | partner-local SKU |
| title | string | yes | product title |
| brand | string | yes | free text |
| category | string | yes | one of the agreed category list |

- **Grain:** one object per (partner_id, sku).
- Root of the document must be a JSON array of objects.
- **Compatibility policy:** same as supplier-prices (additive fields are rejected until the contract is versioned).
- **Freshness (draft):** daily by 06:00 UTC.

### historical-orders (Parquet)

| Field | Type | Required | Notes |
|---|---|---|---|
| order_id | integer | yes | source order identifier |
| customer_id | integer | yes | source customer identifier |
| status | string | yes | pending / paid / shipped / delivered / cancelled |
| order_total | decimal ≥ 0 | yes | 2 fractional digits |
| order_date | date | yes | order placement date |

- **Grain:** one row per order_id.
- Physical layout: typed Parquet columns (int64, decimal(12,2), date32), snappy.
- **Compatibility policy:** same column rules as CSV sources; partition/row-group layout may change freely.
- **Freshness (draft):** weekly full extract, Monday by 06:00 UTC.

### supplier-stock (XLSX)

| Field | Type | Required | Notes |
|---|---|---|---|
| supplier_id | string | yes | supplier slug |
| sku | string | yes | supplier-local SKU |
| quantity | integer ≥ 0 | yes | units on hand |
| updated_at | date | yes | stock snapshot date |

- **Grain:** one row per (supplier_id, sku) per snapshot.
- Physical layout: first worksheet, header row first; cell types per column (date cells as Excel dates).
- **Compatibility policy:** same column rules as CSV sources; sheet renaming is allowed, extra sheets are ignored.
- **Freshness (draft):** daily by 06:00 UTC.

---

## API sources

All API sources share the same mechanics:

| Aspect | Value |
|---|---|
| Endpoint | mock service `MOCK_API_BASE_URL` (ADR 0002); production-like pagination contracts |
| Raw archive | `s3://archive/api/<source>/<yyyymmdd>/page_XXXX.json` (unchanged response bodies) |
| Batch identity | `batch_id = <source>-<yyyymmdd>` (logical-date-addressed) |
| Duplicate policy | deterministic page keys; a re-run of the same date overwrites the same objects |
| Retry policy | 429/5xx/timeout retried with exp backoff + jitter, `Retry-After` honored; other 4xx fail fast |
| Metadata | `archive/_manifests/<source>/<source>-<yyyymmdd>.json` |

### fx-rates

- `GET /api/v1/fx-rates?base=EUR&date=YYYY-MM-DD&page=&page_size=` (offset pagination).
- Record: `currency` (string), `rate` (float). Grain: one quote per (date, currency).
- **Compatibility policy:** envelope fields (`rates`, `total_count`) are required; record fields may only be added with a schema-version bump.
- **Freshness (draft):** daily by 00:30 UTC for the previous day.

### marketing-campaigns

- `GET /api/v1/marketing/campaigns?status=&page=&page_size=` (offset pagination).
- Record: `campaign_id`, `name`, `channel`, `status`, `start_date`, `end_date`, `budget_eur`, `spend_eur`, `impressions`, `clicks`. Grain: one row per campaign_id (current snapshot).
- **Compatibility policy:** envelope fields (`campaigns`, `total_count`) are required; record fields may only be added with a schema-version bump.
- **Freshness (draft):** daily by 02:00 UTC.

### deliveries

- `GET /api/v1/deliveries?updated_since=&cursor=&limit=` (cursor pagination).
- Record: `delivery_id`, `order_id`, `carrier`, `status`, `shipped_at`, `delivered_at`, `updated_at`. Grain: latest state per delivery_id; corrections arrive as updated `updated_at` rows.
- **Compatibility policy:** envelope fields (`deliveries`, `next_cursor`) are required.
- **Freshness (draft):** every 15 minutes.

---

## PostgreSQL CDC raw events

| Aspect | Contract |
|---|---|
| Source tables | exactly `public.customers`, `public.orders`, `public.payments` |
| Topics | `omni.oltp.public.<table>`; one partition per table |
| Operations | `r` snapshot read, `c` create, `u` update, `d` delete |
| Grain | one row per `(kafka_topic, kafka_partition, kafka_offset)` |
| Sink | `iceberg.bronze.postgres_cdc_events`, append-only insert semantics |
| Identity | SHA-256 `event_id` of the transport coordinate |
| Ordering | transport order per single-partition table topic; source LSN retained; event/ingestion time may move backwards; no cross-table total-order claim |
| Raw payload | exact decoded key and Debezium envelope JSON retained as text |
| Delivery | at-least-once; Iceberg MERGE before synchronous Kafka offset commit |
| Invalid record | fail-stop with topic/partition/offset context; offset is not committed |
| Retention | Kafka: 7 days or 5 GiB per data-topic partition; automated Iceberg history expiration is not delivered (see TD-006 in `../PROGRESS.md`) |

Each route has one exact primary-key contract: `customers.customer_id`,
`orders.order_id`, or `payments.payment_id`. The Kafka key must contain exactly
that field as a JSON integer, and its value must match the `after` row for `r/c/u` or the
`before` row for `d`; violations fail-stop before covered offsets advance.
Delete events require a `before` object and null `after`. PostgreSQL default
primary-key replica identity means non-key old values are not guaranteed.
Kafka tombstones are configured off (`tombstones.on.delete=false`) and treated
as contract failures if received.

Compatibility at the raw boundary is explicit:

| Source change | Raw CDC behavior | Compatibility |
|---|---|---|
| Add nullable/defaulted non-key field | accepted and preserved in envelope/`before`/`after` JSON; no Bronze DDL | compatible |
| Primary-key shape/value or route change | validation failure; offset not committed | incompatible |
| Non-key rename/drop/type change | raw JSON is preserved but no automatic classification is possible | downstream-breaking; reviewed migration required |
| Malformed envelope/unsupported operation/tombstone | validation failure; offset not committed | incompatible |

Schema-less JSON is intentional: it preserves additive payload evolution
without another platform service, but it is not a schema registry. Typed
current-state models must version and test the fields they project. The raw
ledger never overwrites by business key, LSN, source timestamp, or ingestion
time, so event-time-out-of-order records remain separate events. Kafka order is
only per table topic; there is no cross-table total order.

Resetting Kafka while retaining Bronze produces a new snapshot with new
transport coordinates; this is new raw history, not a duplicate under the
transport identity. A clean end-to-end replay therefore requires a separate,
explicit Bronze reset.

## PostgreSQL CDC typed/current state

The raw ledger is projected into three event-grain staging views and three
live-key current-state views:

| Entity | Typed event model | Current-state model | Business key |
|---|---|---|---|
| customers | `silver.stg_cdc_customers` | `silver.int_cdc_customers_current` | `customer_id` |
| orders | `silver.stg_cdc_orders` | `silver.int_cdc_orders_current` | `order_id` |
| payments | `silver.stg_cdc_payments` | `silver.int_cdc_payments_current` | `payment_id` |

Typed staging uses strict conversions: identifiers are `bigint`, order/payment
money is `decimal(12,2)`, and source row timestamps are
`timestamp(6) with time zone`. Required fields on `r/c/u` are dbt-tested;
invalid types fail casts rather than becoming silent NULLs. Delete events
retain the business key and event metadata only because default PostgreSQL
replica identity does not promise complete old non-key values.

Current state ranks an initial `r` as the pre-streaming baseline even when it
carries an LSN. Streamed events then rank by `source_lsn DESC`, followed by the
single-partition table-topic `kafka_offset DESC`, with `event_id` as a
deterministic final tie-break. The winner is filtered only after ranking: a
winning `d` removes the key; an older delete cannot hide a later recreate.
`source_tx_id`, source/Kafka timestamps, row `updated_at`, and ingestion time
are audit fields, never ordering keys. A streamed `c/u/d` with null LSN is
incompatible and fails the dbt build. This requires the ADR 0006
one-partition-per-table contract; there is no cross-topic Kafka order.

Additive unknown non-key fields remain compatible and preserved in Bronze but
are ignored by typed views until their projection is reviewed. Missing
required projected fields or incompatible types are downstream-breaking and
fail dbt tests/build. Snapshot-backed Silver models remain available for
rollback and uncaptured child attributes, but they are not unioned into the
captured Gold entities.

## CDC-backed Gold contract

| Dataset | Grain / key | Ownership and delete semantics |
|---|---|---|
| `gold.dim_customer` | one committed non-delete version; `customer_key = <customer_id>_<event_id>` | full customer CDC history; delete closes the prior version and emits no row; recreate begins a new lifecycle |
| `gold.fact_orders` | one live order; `order_id` | `int_cdc_orders_current`; winning delete removes the row |
| `gold.fact_payments` | one live payment; `payment_id` | `int_cdc_payments_current`; winning delete removes the row |
| `gold.fact_order_items` | one captured snapshot line with a live parent | snapshot attributes, inner-joined to `fact_orders` |
| `gold.fact_shipments` | one captured snapshot shipment with a live parent | snapshot attributes, inner-joined to `fact_orders` |

Customer SCD2 validity is system/source sequence, not business-effective time:

- `valid_from_lsn` is inclusive and null only for the initial `r` baseline;
- `valid_to_lsn` is the exclusive next customer transaction boundary and null
  only for an open live version;
- same-LSN customer events collapse to the final customer-topic offset;
- source timestamps are audit-only and may regress;
- a live customer has one current version, while a deleted customer has zero.

An order's `customer_key` is frozen at the current order lifecycle start. An
`r` order uses the customer baseline. A `c` order uses the unique customer
interval containing the create LSN, including a customer version beginning at
the same LSN. Later order updates do not re-key it. There is no earliest/current
fallback: an unresolved or ambiguous version fails uniqueness, not-null,
relationship, and key-set tests.

The child contract is deliberately hybrid. Stale snapshot lines and shipments
disappear immediately with a deleted CDC parent, but children for a new live
order can be absent until the next snapshot. Consumers must not interpret
missing children during that window as proof that the source order has none.

Orders/payments reconciliation remains fail-loud. Automatic Airflow builds
capture a stable consumed boundary for exactly the three configured topics and
pass it to dbt as `cdc_boundary`. Each topic entry contains partition `0` and a
non-negative `offset_exclusive`; CDC staging reads only
`kafka_offset < offset_exclusive`. Exact topic coverage, partition topology,
and integer offsets are validated before dbt starts. Two lag-zero samples must
have identical broker high watermarks across the configured stability window.
Records appended to Bronze after capture remain outside that build and become
eligible only after a later boundary advances.

The boundary is a per-topic quiescent transport frontier, not a cross-topic
total order, source transaction, or exactly-once guarantee. PostgreSQL LSN
remains the entity/source ordering key and orders/payments reconciliation can
still fail a semantically incomplete state. The host-side unbounded dbt command
is an operator recovery/diagnostic path that requires the runbook's explicit
stable-lag check and must not overlap the coordinated Airflow DAG.

Compatibility: replacing the former date-validity columns on `dim_customer`
with LSN validity is the coordinated breaking change ratified by ADR 0007.
Mart schemas remain unchanged, and ClickHouse continues to mirror those marts
through a full-snapshot swap.

## Change process

1. The producer announces the change; the owner classifies it (additive vs breaking).
2. Breaking changes require: contract update here, schema version bump where the producer exposes one, validator/client update, tests, and downstream impact review.
3. For PostgreSQL raw CDC only, additive nullable/defaulted non-key fields are accepted and preserved automatically; typed consumers still require an explicit reviewed projection change.
4. Other source validators keep their source-specific strictness documented above; raw CDC compatibility does not weaken file or API contracts.

Runbook for handling rejected supplier files: `docs/runbooks/bad-supplier-file.md`.
