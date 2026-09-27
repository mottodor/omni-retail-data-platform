{{ config(materialized='view') }}

-- Grain: one row per order line (lines are immutable after creation).
with ranked as (
    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        line_total,
        created_at,
        updated_at,
        row_number() over (
            partition by order_item_id
            order by _batch_date desc, _ingested_at desc
        ) as version_rank
    from {{ ref('stg_order_items') }}
)
select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    line_total,
    created_at
from ranked
where version_rank = 1
