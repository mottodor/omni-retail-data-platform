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
