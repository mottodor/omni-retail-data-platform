{{ config(materialized='view') }}

-- Grain: one row per OLTP snapshot record (order_item_id, _batch_id).
select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    line_total,
    created_at,
    updated_at,
    _batch_id,
    _batch_date,
    _source_object,
    _ingested_at
from {{ source('bronze', 'order_items') }}
