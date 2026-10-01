# OLTP source data model (Phase 2)

Source of truth for the synthetic e-commerce OLTP database running in the
`postgres` service (`POSTGRES_DB=omni_oltp`). DDL: `postgres/init/01_oltp_schema.sql`
(idempotent; applied on first volume init or by `make generate-oltp`).

## Tables

| Table | Grain | PK | Notes |
|---|---|---|---|
| `categories` | one row per product category | `category_id` | self-referencing `parent_category_id` (5 roots, 20 leaves) |
| `products` | one row per sellable product | `product_id` | unique `sku`; `unit_cost <= unit_price` (CHECK); `is_active` |
| `customers` | one row per registered customer | `customer_id` | unique `email`; `status` ∈ active/inactive/churned; `segment` ∈ standard/premium/vip |
| `orders` | one row per order | `order_id` | `status` ∈ pending/paid/shipped/delivered/cancelled/refunded; `order_total = Σ line_total + shipping_cost` (enforced by generator); currency USD/EUR/GBP |
| `order_items` | one row per (order, product) | `order_item_id` | unique `(order_id, product_id)`; `line_total = quantity × unit_price` (enforced by generator); `ON DELETE CASCADE` from `orders` |
| `payments` | one row per order (1:1) | `payment_id` | unique `order_id`, unique `transaction_id`; `status` ∈ pending/authorized/captured/failed/refunded/cancelled; cascade delete with order |
| `shipments` | one row per shipped order (1:1) | `shipment_id` | unique `order_id`, unique `tracking_number`; `status` ∈ pending/in_transit/delivered/cancelled; exists only for shipped/delivered/late-refunded orders |

Every table carries `created_at`/`updated_at` (`timestamptz`). Indexes cover
foreign keys plus `orders.created_at`/`orders.updated_at` and
`customers.updated_at` for future watermark-based incremental extraction and
CDC.

## Status model

```text
orders:     pending ──> paid ──> shipped ──> delivered
               │          │         │            │
               │          │         └────────────┼──> refunded (late)
               │          └──> cancelled/refunded (pre-shipment refund)
               └──> cancelled (never paid)

payments:   pending ──> captured ──> refunded
               │
               └──> cancelled / failed

shipments:  (created at paid ──> shipped as in_transit) ──> delivered
```

Generator invariants (tested in `tests/unit/generators/oltp/`):

- every order has ≥ 1 item and exactly 1 payment;
- `order_total = Σ line_total + shipping_cost`, `line_total = quantity × unit_price`;
- payment status is consistent with order status (pending↔pending,
  paid/shipped/delivered↔captured, refunded↔refunded, cancelled↔cancelled|refunded);
- shipments exist only for shipped/delivered/late-refunded orders with
  `created_at ≤ shipped_at ≤ delivered_at`;
- all initial-load timestamps are ≤ the fixed anchor (2026-09-10 12:00 UTC).

## Generator

Deterministic: single `random.Random(seed)` instance, fixed anchor timestamp.
Same seed (and volumes) ⇒ identical dataset; verified by an in-database
fingerprint across truncate/reload cycles.

- Initial load: `make generate-oltp`
  (seed 42, 10 000 customers, 5 000 products, 100 000 orders, 365 days of
  history, ~50 s end-to-end including inserts).
- Continuous workload: `make mutate-oltp EVENTS=N` — one batch with mix
  40 % order-status updates, 35 % new orders, 10 % hard deletes
  (pending orders only, cascading), 10 % customer updates, 5 % product price
  updates. The plan is a pure deterministic function of
  (seed, database snapshot, event time).
- Updates are guarded (`WHERE status = <expected>`) so replays cannot corrupt
  state; `updated_at` changes only when a row actually changes.

## Default-load source metrics (seed 42)

| Metric | Value |
|---|---|
| categories | 25 (5 root, 20 leaf) |
| products | 5 000 |
| customers | 10 000 |
| orders | 100 000 |
| order_items | 253 514 (~2.54 per order) |
| payments | 100 000 |
| shipments | 89 406 |
| order date range | 2025-09-10 → 2026-09-10 (anchor-based) |

Order status distribution (initial load, aged realistically):

| Status | Share |
|---|---|
| delivered | ~85.6 % |
| cancelled | ~5.3 % |
| refunded | ~4.8 % |
| paid | ~2.5 % |
| shipped | ~1.5 % |
| pending | ~0.5 % |

Customer status: 85 % active / 10 % inactive / 5 % churned.
Segment: 70 % standard / 25 % premium / 5 % vip.
Currency: 85 % USD / 10 % EUR / 5 % GBP. Item popularity follows a power law
(`weight ∝ 1/rank^0.7`) so downstream "top products" analytics are meaningful.

## Bronze layer (Phase 5 slice 1)

Ten Iceberg tables (`bronze` schema) mirror the raw archive objects one
logical day at a time, partitioned by `_batch_date`:

| Table | Source | Grain |
|---|---|---|
| `orders`, `order_items`, `customers`, `products`, `categories`, `payments`, `shipments` | PG snapshots (Parquet) | one row per source record per logical batch |
| `fx_rates`, `campaigns`, `deliveries` | API pages (JSON, flattened) | one row per source record per raw page |

Service columns on every table: `_batch_id` (deterministic
`<source>-<yyyymmdd>`), `_batch_date` (partition), `_source_object`,
`_source_object_row_position` (zero-based coordinate inside the immutable raw
object), `_ingested_at`. Explicit-date loads are idempotent per (table,
logical date): `DELETE` partition → batched `INSERT`, row-count-verified
against the raw manifest. Watermark-driven `run-new` resumes at independently
replaceable source-object row ranges and verifies the manifest count plus
unique raw coordinates.

## PostgreSQL CDC Bronze ledger (Phase 8)

`iceberg.bronze.postgres_cdc_events` is an append-only raw event ledger for
`public.customers`, `public.orders`, and `public.payments`. Its grain is one
row per Kafka transport coordinate `(kafka_topic, kafka_partition,
kafka_offset)`. `event_id` is the deterministic SHA-256 encoding of that
coordinate and is the insert-only MERGE key, so replay is a no-op.

| Column group | Fields | Semantics |
|---|---|---|
| Identity | `event_id`, `kafka_topic`, `kafka_partition`, `kafka_offset` | stable transport identity; not a business key |
| Kafka time | `kafka_timestamp` | broker record timestamp; fallback for partition date |
| Source routing | `source_schema`, `source_table`, `operation` | allow-listed `public` table; `r/c/u/d` |
| Source ordering | `source_lsn`, `source_tx_id`, `source_timestamp` | retained for later current-state derivation; LSN is not globally unique |
| Raw payload | `key_json`, `envelope_json` | exact UTF-8-decoded schema-less JSON from Kafka |
| Convenience payload | `before_json`, `after_json` | parsed object JSON; nullable according to operation |
| Lakehouse metadata | `event_date`, `ingested_at` | source timestamp date (Kafka fallback), UTC ingestion time |

The Iceberg table is partitioned by `event_date`. Deletes retain the Debezium
key and delete envelope; because PostgreSQL keeps default primary-key replica
identity, non-key old values are not guaranteed. Broker tombstones are disabled
and fail loudly if one nevertheless arrives. Route-specific primary-key shape
and row/key equality are validated before offset commit.

Additive nullable/defaulted non-key source columns require no Bronze DDL: the
new fields remain inside the immutable raw JSON, so old and new payload shapes
can coexist. Non-key rename/drop/type changes are raw-capturable but are
breaking for typed consumers and require review. Events remain distinct by
transport coordinate even when source/event time moves backwards; the sink
does not sort or overwrite by business key, LSN, or timestamp, and it makes no
cross-topic total-order claim.

This table does not replace the snapshot-shaped
`bronze.customers/orders/payments` tables. It now feeds parallel typed/current-
state Silver models; switching the established Gold path remains a separate
Phase 8 cutover.

## CDC typed/current-state Silver (Phase 8)

Three event-grain views strictly type the schema-less raw payload while
retaining source/transport metadata:

| Model | Grain | Typed payload |
|---|---|---|
| `stg_cdc_customers` | one row per customer CDC event | customer attributes and timestamps |
| `stg_cdc_orders` | one row per order CDC event | order attributes, `decimal(12,2)` amounts, timestamps |
| `stg_cdc_payments` | one row per payment CDC event | payment attributes, `decimal(12,2)` amount, timestamps |

For `r/c/u`, values come only from `after_json`; required source fields are
dbt-tested and incompatible types fail strict casts. A `d` row retains only
the typed business key and event metadata because default replica identity does
not guarantee complete non-key old values. Additive unknown fields remain in
Bronze and do not change these projections until reviewed.

The corresponding `int_cdc_<entity>_current` views have one row per live
business key. They rank all operations before filtering deletes:

```text
source_lsn DESC NULLS LAST -> kafka_offset DESC -> event_id DESC
```

LSN is the PostgreSQL source-order boundary; offset resolves snapshot/null-LSN
and same-LSN events inside each single-partition table topic. Event time, row
`updated_at`, Kafka time, transaction ID, and ingestion time do not select
state. A winning delete removes the key, while a later recreate wins normally.
There is no cross-topic order claim.

These models deliberately run alongside the established snapshot-backed
`stg_*`/`int_*` and Gold models. Gold cutover must first define customer SCD2
delete intervals and stale snapshot-only `order_items`/`shipments` behavior.

## Silver layer (Phase 5 slice 2)

Staging (`stg_*`) is a typed pass-through of bronze. Intermediate
(`int_*`) derives the **current state** of each entity: append-only bronze
history is deduplicated by `row_number() ... order by _batch_date desc,
_ingested_at desc` on the business key. `int_fx_rates_daily` keeps the
latest rate per (currency, day); `int_campaigns` keeps the latest daily
campaign snapshot; `int_deliveries` keeps the latest carrier status.

Known limitation of this established path: snapshot extraction cannot see hard
deletes, so a deleted source row retains its last version in the snapshot-backed
`int_*` and downstream facts/dimensions. The parallel Phase 8
`int_cdc_*_current` views close that gap for their three entities, but Gold has
not switched to them yet.

## Gold layer — Kimball (Phase 5 slice 2)

### Dimensions

| Model | Grain | PK | Type | Upstream |
|---|---|---|---|---|
| `dim_date` | calendar day | `date_key` (yyyymmdd int) | generated | `int_orders` bounds (created/updated) |
| `dim_product` | product | `product_id` | SCD1 | `int_products`, `int_categories` |
| `dim_campaign` | campaign | `campaign_id` | SCD1 | `int_campaigns` |
| `dim_customer` | customer **version** | `customer_key` | SCD2 | `stg_customers` history |

`dim_customer` SCD2 semantics:

- every bronze row of a customer is one version (incremental batches carry
  only changed rows);
- `valid_from` = `_batch_date` of the version; `valid_to` = the day before
  the next version (inclusive); open versions use `9999-12-31`;
- `customer_key = '<customer_id>_<valid_from>'` — deterministic, rebuildable;
- `is_current` marks the single open version (tested);
- daily grain: multiple same-day source changes collapse into that day's
  version.

### Facts

| Model | Grain | PK | FKs | Measures | Notes |
|---|---|---|---|---|---|
| `fact_orders` | order | `order_id` | `customer_key` → `dim_customer` (point-in-time on `created_at`, earliest-version fallback) | `shipping_cost`, `order_total` (source currency) | degenerate `customer_id` |
| `fact_order_items` | order line | `order_item_id` | `order_id` → `fact_orders`; `product_id` → `dim_product` | `quantity`, `unit_price`, `line_total` | immutable |
| `fact_payments` | payment | `payment_id` | `order_id` → `fact_orders` | `payment_amount` | degenerate `transaction_id`; reconciled vs orders by test |
| `fact_shipments` | shipment | `shipment_id` | `order_id` → `fact_orders` | — | degenerate `tracking_number` |

### Business tests (dbt singular)

- orders ↔ payments: exactly one payment per order, equal amounts,
  consistent status pairs, no orphans;
- SCD2: non-overlapping contiguous intervals; exactly one current version;
- non-negative amounts; strictly positive FX rates;
- temporal ordering (created ≤ updated; shipped ≤ delivered; payment
  created ≥ order created; campaign start ≤ end);
- `line_total = quantity × unit_price`.

### Known limitations

- delivery-API `order_id` values are the partner's synthetic references
  ("ORD-…") and never join OLTP orders; `int_deliveries` feeds
  `mart_delivery_performance` (slice 3) only;
- currency normalization to EUR happens in marts (slice 3) via
  `int_fx_rates_daily`; facts keep source currency;
- snapshot extracts hide hard deletes until CDC (Phase 8);
- SCD2 versions have daily granularity (see above).

## Analytics marts (Phase 5 slice 3)

The `analytics` schema holds the business-facing marts for the four planned
dashboards. All EUR measures are normalized through `int_orders_fx` — an
order-grain intermediate view shared by the marts: `rate_to_eur` is the latest
`int_fx_rates_daily` rate on or before the order date ("latest", not "exact
day", so ingestion gaps do not strand orders), in units of the order currency
per 1 EUR; the fx API quotes no EUR row, so EUR orders carry rate 1.0. An
order older than every rate keeps `rate_to_eur` NULL and is surfaced by the
`marts_fx_rate_coverage` business test (fail-loud policy). Facts keep source
currency.

| Mart | Grain | PK | Measures | Notes |
|---|---|---|---|---|
| `mart_daily_sales` | one row per (order date, product category, customer region) | the combination (order_date, category_name, region) | `orders_count`, `items_sold`, `revenue_eur`, `margin_eur` | realized-revenue policy; `orders_count` additivity caveat (below) |
| `mart_customer_ltv` | one row per customer with at least one order | `customer_id` | `orders_count`, `gmv_eur`, `avg_order_value_eur` | `region`/`segment` from the customer's **current** SCD2 version |
| `mart_marketing_roi` | one row per campaign (current API snapshot) | `campaign_id` | `spend_eur`, `budget_eur`, `impressions`, `clicks`, `ctr`, `cpc_eur`, `cpm_eur`, `budget_utilization` | no ROAS — campaigns are not linked to orders (see below) |
| `mart_delivery_performance` | one row per carrier (delivery-partner API feed) | `carrier` | `delivery_count`, per-status counts, `avg_transit_hours`, `last_update_at` | independent of `fact_shipments` (see below) |

Per-mart semantics (source of truth: `dbt/models/marts/schema.yml`):

- `mart_daily_sales` — GMV/revenue/margin roll-ups for the Revenue and Sales
  dashboards. Financial measures (`items_sold`, `revenue_eur`, `margin_eur`)
  cover realized orders only (cancelled/refunded excluded); `orders_count`
  includes all orders. **`orders_count` is additive across dates but NOT
  across category/region** — a multi-category order counts once per row.
  `revenue_eur` is item-line revenue excluding shipping (shipping is
  order-grain and stays in `fact_orders`/`int_orders_fx`). `region` is the
  customer's point-in-time version (order `customer_key` → `dim_customer`).
  Known simplification (mock source): product prices are currency-naive; line
  amounts are normalized with the order currency's FX rate.
- `mart_customer_ltv` — retention/LTV/activation analysis. `gmv_eur` is
  realized order value (`order_total` incl. shipping, EUR); `orders_count`
  counts every order (activity); `avg_order_value_eur` = `gmv_eur` / realized
  orders — NULL only for customers whose every order is cancelled/refunded.
  Reporting today's segmentation, not point-in-time, is a documented choice.
- `mart_marketing_roi` — spend/CTR/CPC/CPM monitoring. Derived media KPIs are
  null-safe (zero impressions/clicks → NULL, never an error);
  `budget_utilization` can exceed 1.0 (the source overspends up to 5 %).
  LIMITATION (spec §2): campaigns are not linked to orders, so revenue
  attribution is impossible with current data — no ROAS is computed and none
  may be faked; ROAS arrives with clickstream attribution (Phase 9).
- `mart_delivery_performance` — carrier scorecard (transit times, status mix).
  Fixed status columns follow the API status domain; the counts-sum business
  test makes an unknown new status loud instead of silently dropped.
  `avg_transit_hours` averages deliveries with both `shipped_at` and
  `delivered_at` (NULL only when the carrier has no completed deliveries
  yet). The feed's `order_id` values are the partner's synthetic references
  ("ORD-…") and never join OLTP orders — this mart is independent of
  `fact_shipments`, which serves OLTP shipments.

## Serving layer (ClickHouse, Phase 6)

The `analytics` database in ClickHouse holds derived serving copies of the
four Gold marts ([ADR 0004](adr/0004-clickhouse-serving-publication.md)).
Iceberg stays the source of truth; every serving table is a 1:1 mirror of
its Gold mart — same columns, no ClickHouse-only business logic — and the
whole layer is rebuildable with one command (`make serving-rebuild`, or
`ARGS="--mart mart_daily_sales"` for a single mart).

### Physical design (ratified in slice 2, guide §26.2)

MergeTree everywhere: the swap-based publication replaces whole snapshots,
so dedup engines (`ReplacingMergeTree` etc.) would add semantics without
adding guarantees. `ORDER BY` follows each mart's BI access pattern;
partitioning exists only where a date filter/partition-replace makes
functional sense; no TTL — historical portfolio data, expiry has no
meaning.

| Serving table (+ `_staging` twin each) | ORDER BY | Partitioning | Rationale |
|---|---|---|---|
| `mart_daily_sales` | `(order_date, category_name, region)` | monthly `toYYYYMM(order_date)` (migration 0007) | the single naturally date-filtered mart; monthly partitions keep the slice-3 `REPLACE PARTITION` option open |
| `mart_customer_ltv` | `(region, segment, customer_id)` | none | full-history snapshot per customer; LTV panels filter region/segment, `first/last_order_date` are attributes, not filters |
| `mart_marketing_roi` | `(channel, start_date, campaign_id)` | none | campaign panels filter channel + date range; hundreds of rows |
| `mart_delivery_performance` | `(carrier)` | none | one row per carrier — the grain is the only access key |

### Type mapping and nullability

Type mapping from Trino: `integer` → `Int32`, `bigint` identifiers →
`Int64`, `bigint` counts → `UInt64`, `date` → `Date`, `varchar` →
`String`, `double` → `Float64`, `decimal(p,s)` → `Decimal(p,s)`,
`timestamp(6) with time zone` → `DateTime64(6, 'UTC')`. Money columns
mirror the Gold types exactly (`Decimal(38, 21)` / `Decimal(38, 6)` —
Trino decimal-division scales included): the exact round-trip is what
makes Gold-vs-CH reconciliation trivially provable, so normalizing the
scales is deliberately rejected (slice 2 decision).

Nullability mirrors the Gold model semantics: `Nullable(..)` only where
the dbt model can produce NULLs — `nullif`-protected divisions
(`ctr`, `cpc_eur`, `cpm_eur`, `budget_utilization`), `case`-without-`else`
(`avg_order_value_eur`, `avg_transit_hours`), optional source attributes
(`end_date`). Everything else is NOT NULL by construction; an unexpected
NULL fails the staging insert loudly instead of corrupting the copy.

### Publication and verification

Publication mode for every mart is a **full snapshot swap**:
`TRUNCATE staging` → insert → `EXCHANGE TABLES` (see
`src/omni_retail/serving/clickhouse/publisher.py`); the registry of all
four marts lives in `specs.py`, cross-checked against the migrations by a
unit test. `superset_reader` has SELECT-only access; `omni_publisher`
holds the write grants; `analytics.schema_migrations` tracks the applied
files from `clickhouse/migrations/`. Per-mart reconciliation (row count +
full row-by-row equality Gold vs serving), republish idempotency, the
one-command full rebuild, and reader permissions are asserted by
`tests/integration/test_serving_publication.py`.

The funnel mart is deferred until its clickstream source exists in
Phase 9.
