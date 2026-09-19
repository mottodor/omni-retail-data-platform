{{ config(materialized='view') }}

-- Grain: one row per order (latest known version).
with ranked as (
    select
        order_id,
        customer_id,
        status,
        currency,
        shipping_cost,
        order_total,
        created_at,
        updated_at,
        row_number() over (
            partition by order_id
            order by _batch_date desc, _ingested_at desc
        ) as version_rank
    from {{ ref('stg_orders') }}
)
select
    order_id,
    customer_id,
    status,
    currency,
    shipping_cost,
    order_total,
    created_at,
    updated_at
from ranked
where version_rank = 1
