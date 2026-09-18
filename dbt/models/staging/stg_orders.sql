{{ config(materialized='view') }}

-- Grain: one row per OLTP snapshot record (order_id, _batch_id).
select
    order_id,
    customer_id,
    status,
    currency,
    shipping_cost,
    order_total,
    created_at,
    updated_at,
    _batch_id,
    _batch_date,
    _source_object,
    _ingested_at
from {{ source('bronze', 'orders') }}
