{{ config(materialized='table') }}

-- Grain: one row per customer with at least one order. region/segment come
-- from the customer's CURRENT SCD2 version (see schema.yml rationale).
-- gmv_eur is realized order value (order_total incl. shipping, EUR);
-- orders_count includes cancelled/refunded orders (activity).
with orders as (
    select
        customer_id,
        order_status,
        order_total_eur,
        date(created_at) as order_date
    from {{ ref('int_orders_fx') }}
),
current_customers as (
    select
        customer_key,
        customer_id,
        region,
        segment
    from {{ ref('dim_customer') }}
    where is_current
),
agg as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        count(*) as orders_count,
        count(*)
            filter (where order_status not in ('cancelled', 'refunded'))
            as realized_orders_count,
        coalesce(
            sum(order_total_eur)
                filter (where order_status not in ('cancelled', 'refunded')),
            0
        ) as gmv_eur
    from orders
    group by customer_id
)
select
    a.customer_id,
    c.customer_key,
    c.region,
    c.segment,
    a.first_order_date,
    a.last_order_date,
    a.orders_count,
    a.gmv_eur,
    case
        when a.realized_orders_count > 0
            then a.gmv_eur / a.realized_orders_count
    end as avg_order_value_eur
from agg as a
join current_customers as c
    on c.customer_id = a.customer_id
