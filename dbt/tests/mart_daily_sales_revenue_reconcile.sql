-- Cross-grain reconciliation (Review Focus 5): per order date, the mart's
-- item-line revenue (EUR, realized orders only) must equal the order-grain
-- truth (order_total - shipping_cost) / rate from int_orders_fx. Different
-- source grain catches fan-out/join bugs; both sides exclude cancelled and
-- refunded orders (realized-revenue policy, Global Constraints).
with mart_side as (
    select
        order_date,
        sum(revenue_eur) as revenue_eur
    from {{ ref('mart_daily_sales') }}
    group by order_date
),
truth_side as (
    select
        date(created_at) as order_date,
        sum((order_total - shipping_cost) / rate_to_eur) as revenue_eur
    from {{ ref('int_orders_fx') }}
    where order_status not in ('cancelled', 'refunded')
      and rate_to_eur is not null
    group by date(created_at)
)
select
    coalesce(m.order_date, t.order_date) as order_date,
    m.revenue_eur as mart_revenue_eur,
    t.revenue_eur as truth_revenue_eur
from mart_side as m
full join truth_side as t
    on t.order_date = m.order_date
where m.order_date is null
   or t.order_date is null
   or abs(m.revenue_eur - t.revenue_eur) > 0.01
