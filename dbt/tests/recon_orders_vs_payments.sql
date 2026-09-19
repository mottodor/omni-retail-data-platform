-- Orders <-> payments reconciliation (generator invariants, enforced as a
-- business contract): every order has exactly one payment, amounts match,
-- and the status pair is consistent. Orphan payments violate too.
with order_side as (
    select
        order_id,
        order_status,
        order_total
    from {{ ref('fact_orders') }}
),
payment_side as (
    select
        order_id,
        count(*) as payment_count,
        max(payment_amount) as payment_amount,
        max(payment_status) as payment_status
    from {{ ref('fact_payments') }}
    group by order_id
)
select
    coalesce(o.order_id, p.order_id) as order_id,
    o.order_status,
    o.order_total,
    p.payment_count,
    p.payment_amount,
    p.payment_status
from order_side as o
full outer join payment_side as p
    on p.order_id = o.order_id
where o.order_id is null
   or p.order_id is null
   or p.payment_count <> 1
   or p.payment_amount <> o.order_total
   or not (
        (o.order_status = 'pending' and p.payment_status = 'pending')
        or (o.order_status in ('paid', 'shipped', 'delivered')
            and p.payment_status = 'captured')
        or (o.order_status = 'refunded' and p.payment_status = 'refunded')
        or (o.order_status = 'cancelled'
            and p.payment_status in ('cancelled', 'refunded'))
   )
