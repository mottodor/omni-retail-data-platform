{{ config(materialized='view') }}

-- Grain: one row per carrier delivery record per raw API page.
-- NOTE: `order_id` is the delivery partner's synthetic reference ("ORD-…")
-- and intentionally does NOT join the OLTP orders (spec §2); this feed
-- serves mart_delivery_performance only.
select
    delivery_id,
    order_id,
    carrier,
    status,
    shipped_at,
    delivered_at,
    updated_at,
    _batch_id,
    _batch_date,
    _source_object,
    _ingested_at
from {{ source('bronze', 'deliveries') }}
