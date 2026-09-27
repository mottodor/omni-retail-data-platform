-- LTV totals reconcile against order-grain truth: total orders_count equals
-- all fact orders; total gmv_eur equals realized order totals (EUR). LTV
-- gmv uses full order_total (incl. shipping) — distinct from
-- mart_daily_sales.revenue_eur (item lines excl. shipping); both policies
-- are documented in their contracts.
with mart_side as (
    select
        sum(orders_count) as orders_count,
        sum(gmv_eur) as gmv_eur
    from {{ ref('mart_customer_ltv') }}
),
truth_side as (
    select
        count(*) as orders_count,
        coalesce(
            sum(order_total_eur)
            filter (where order_status not in ('cancelled', 'refunded')),
            0
        ) as gmv_eur
    from {{ ref('int_orders_fx') }}
    where rate_to_eur is not null
)
select
    m.orders_count as mart_orders_count,
    t.orders_count as truth_orders_count,
    m.gmv_eur as mart_gmv_eur,
    t.gmv_eur as truth_gmv_eur
from mart_side as m
cross join truth_side as t
where m.orders_count <> t.orders_count
   or abs(m.gmv_eur - t.gmv_eur) > 0.01
