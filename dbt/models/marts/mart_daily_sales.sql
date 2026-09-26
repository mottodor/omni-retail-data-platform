{{ config(materialized='table') }}

-- Grain: one row per (order date, product category, customer region).
-- Financial measures (revenue_eur, margin_eur, items_sold) cover realized
-- orders only (excludes cancelled/refunded); orders_count counts every
-- order of the group. Region is the customer's point-in-time version
-- (order customer_key -> dim_customer). See schema.yml for the additive
-- orders_count caveat across category/region rows.
with lines as (
    select
        i.order_id,
        fx.customer_key,
        fx.order_status,
        fx.rate_to_eur,
        fx.order_status not in ('cancelled', 'refunded') as is_realized,
        date(fx.created_at) as order_date,
        i.quantity,
        i.line_total / fx.rate_to_eur as line_total_eur,
        (i.unit_price - p.unit_cost) * i.quantity / fx.rate_to_eur as line_margin_eur,
        p.category_name
    from {{ ref('fact_order_items') }} as i
    join {{ ref('int_orders_fx') }} as fx
        on fx.order_id = i.order_id
    join {{ ref('dim_product') }} as p
        on p.product_id = i.product_id
),
enriched as (
    select
        l.order_id,
        l.order_status,
        l.is_realized,
        l.order_date,
        l.category_name,
        c.region,
        l.quantity,
        l.line_total_eur,
        l.line_margin_eur
    from lines as l
    join {{ ref('dim_customer') }} as c
        on c.customer_key = l.customer_key
)
select
    d.date_key,
    e.order_date,
    e.category_name,
    e.region,
    count(distinct e.order_id) as orders_count,
    coalesce(sum(e.quantity) filter (where e.is_realized), 0) as items_sold,
    coalesce(sum(e.line_total_eur) filter (where e.is_realized), 0) as revenue_eur,
    coalesce(sum(e.line_margin_eur) filter (where e.is_realized), 0) as margin_eur
from enriched as e
join {{ ref('dim_date') }} as d
    on d.full_date = e.order_date
group by
    d.date_key,
    e.order_date,
    e.category_name,
    e.region
