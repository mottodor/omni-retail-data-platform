{{ config(materialized='view') }}

-- Grain: one row per order. Order-grain FX normalization primitive shared
-- by mart_daily_sales (line-level division) and mart_customer_ltv.
-- rate semantics: units of the order currency per 1 EUR (fx API, base EUR);
-- the API quotes no EUR row, so EUR orders carry rate 1.0. "Latest rate on
-- or before the order date" is chosen over "exact day" so ingestion gaps do
-- not strand orders; an order older than every rate keeps rate NULL and is
-- surfaced by the marts_fx_rate_coverage business test (fail-loud policy).
with matched as (
    select
        o.order_id,
        o.customer_id,
        o.customer_key,
        o.currency,
        o.order_status,
        o.order_total,
        o.shipping_cost,
        o.created_at,
        f.rate,
        f.rate_date,
        row_number() over (
            partition by o.order_id
            order by f.rate_date desc
        ) as version_rank
    from {{ ref('fact_orders') }} as o
    left join {{ ref('int_fx_rates_daily') }} as f
        on f.currency = o.currency
       and f.rate_date <= date(o.created_at)
),
normalized as (
    select
        order_id,
        customer_id,
        customer_key,
        currency,
        order_status,
        order_total,
        shipping_cost,
        created_at,
        case when currency = 'EUR' then 1.0 else rate end as rate_to_eur
    from matched
    where version_rank = 1
)
select
    order_id,
    customer_id,
    customer_key,
    currency,
    order_status,
    order_total,
    shipping_cost,
    created_at,
    rate_to_eur,
    order_total / rate_to_eur as order_total_eur,
    shipping_cost / rate_to_eur as shipping_cost_eur
from normalized
