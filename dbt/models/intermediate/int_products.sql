{{ config(materialized='view') }}

-- Grain: one row per product (latest known version).
with ranked as (
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
        row_number() over (
            partition by product_id
            order by _batch_date desc, _ingested_at desc
        ) as version_rank
    from {{ ref('stg_products') }}
)
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
    updated_at
from ranked
where version_rank = 1
