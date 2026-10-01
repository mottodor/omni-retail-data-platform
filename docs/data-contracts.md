# Data Contracts — DRAFT

- **Status:** draft (ownership and SLA numbers will be finalized in Phase 10 — Data Quality & contracts)
- **Scope:** Phase 3 external batch sources plus the Phase 8 PostgreSQL raw CDC ledger
- **Enforcement today:** schema definitions in `omni_retail.ingestion.files.schemas`, API envelope checks, and fail-stop Debezium envelope/route validation in `omni_retail.streaming.cdc`; file violations quarantine data, while malformed CDC records block offset advancement

Additional internal modeled datasets will receive contracts in later phases.

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
| Ordering | transport order per table partition; source LSN retained; no cross-table total-order claim |
| Raw payload | exact decoded key and Debezium envelope JSON retained as text |
| Delivery | at-least-once; Iceberg MERGE before synchronous Kafka offset commit |
| Invalid record | fail-stop with topic/partition/offset context; offset is not committed |
| Retention | Kafka: 7 days or 5 GiB per data-topic partition; Iceberg history has no Phase 8 expiry |

Delete events require a non-empty key object, a `before` object, and null
`after`. PostgreSQL default primary-key replica identity means the key is
reliable but non-key old values are not guaranteed. Kafka tombstones are
configured off (`tombstones.on.delete=false`) and treated as contract failures
if received. Resetting Kafka while retaining Bronze produces a new snapshot
with new transport coordinates; this is new raw history, not a duplicate under
the transport identity. A clean end-to-end replay therefore requires a
separate, explicit Bronze reset.

Schema-less JSON is intentional for this first slice. Additive or incompatible
source-schema changes are fail-loud/unsupported until the later Phase 8 schema
evolution slice; no silent field-dropping compatibility promise is made.

## Change process

1. The producer announces the change; the owner classifies it (additive vs breaking).
2. Breaking changes require: contract update here, schema version bump, validator/client update, tests, and downstream impact review.
3. Additive changes are still rejected by validators until the contract is versioned — an explicit, reviewed switch, never a silent drift.

Runbook for handling rejected supplier files: `docs/runbooks/bad-supplier-file.md`.
