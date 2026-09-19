{{ config(materialized='table') }}

-- Grain: one row per order line. PK: order_item_id.
-- Measures: quantity, unit_price, line_total (line_total = quantity *
-- unit_price is business-tested). Upstream: int_order_items.
select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    line_total,
    created_at
from {{ ref('int_order_items') }}
