{{ config(materialized='table') }}

-- Grain: one row per order (current state). PK: order_id.
-- Measures: shipping_cost, order_total (source currency; FX normalization
-- happens in marts, slice 3). Degenerate natural key: customer_id.
-- customer_key resolves the SCD2 version valid on the order's creation
-- date (point-in-time join); orders predating a customer's first observed
-- version fall back to that earliest version (defensive — the bootstrap
-- full snapshot covers all pre-existing customers).
with matched_keys as (
    select
        o.order_id,
        d.customer_key
    from {{ ref('int_orders') }} as o
    join {{ ref('dim_customer') }} as d
        on d.customer_id = o.customer_id
       and d.valid_from <= date(o.created_at)
       and date(o.created_at) <= d.valid_to
),
earliest_keys as (
    select
        customer_id,
        min(valid_from) as first_valid_from
    from {{ ref('dim_customer') }}
    group by customer_id
)
select
    o.order_id,
    coalesce(
        m.customer_key,
        cast(o.customer_id as varchar) || '_'
            || substr(cast(e.first_valid_from as varchar), 1, 10)
    ) as customer_key,
    o.customer_id,
    o.status as order_status,
    o.currency,
    o.shipping_cost,
    o.order_total,
    o.created_at,
    o.updated_at
from {{ ref('int_orders') }} as o
left join matched_keys as m
    on m.order_id = o.order_id
left join earliest_keys as e
    on e.customer_id = o.customer_id
