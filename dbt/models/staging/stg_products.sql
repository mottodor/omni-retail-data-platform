{{ config(materialized='view') }}

-- Grain: one row per OLTP snapshot record (product_id, _batch_id).
select
    product_id,
    sku,
    name,
    category_id,
    brand,
    unit_price,
    unit_cost,
    is_active,
    created_at,
    updated_at,
    _batch_id,
    _batch_date,
    _source_object,
    _ingested_at
from {{ source('bronze', 'products') }}
