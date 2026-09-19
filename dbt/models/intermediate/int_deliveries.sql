{{ config(materialized='view') }}

-- Grain: one row per carrier delivery (latest known version).
-- `order_id` is the delivery partner's synthetic reference (not OLTP).
with ranked as (
    select
        delivery_id,
        order_id,
        carrier,
        status,
        shipped_at,
        delivered_at,
        updated_at,
        row_number() over (
            partition by delivery_id
            order by _batch_date desc, _ingested_at desc
        ) as version_rank
    from {{ ref('stg_deliveries') }}
)
select
    delivery_id,
    order_id,
    carrier,
    status,
    shipped_at,
    delivered_at,
    updated_at
from ranked
where version_rank = 1
