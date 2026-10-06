{{ config(materialized='table') }}

-- Grain: one row per snapshot order line whose CDC parent order is live.
-- Snapshot attributes remain authoritative for this uncaptured child table;
-- the parent gate removes stale rows after an order cascade delete.
select
    i.order_item_id,
    i.order_id,
    i.product_id,
    i.quantity,
    i.unit_price,
    i.line_total,
    i.created_at
from {{ ref('int_order_items') }} as i
join {{ ref('fact_orders') }} as o
    on o.order_id = i.order_id
