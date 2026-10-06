-- Gold carries exactly the live CDC order/payment key sets. A missing row can
-- indicate an unresolved lifecycle/customer version; an extra row can indicate
-- stale snapshot fallback or incorrect delete handling.
select
    coalesce(c.order_id, g.order_id) as business_key,
    'orders' as entity
from {{ ref('int_cdc_orders_current') }} as c
full outer join {{ ref('fact_orders') }} as g
    on g.order_id = c.order_id
where c.order_id is null or g.order_id is null

union all

select
    coalesce(c.payment_id, g.payment_id) as business_key,
    'payments' as entity
from {{ ref('int_cdc_payments_current') }} as c
full outer join {{ ref('fact_payments') }} as g
    on g.payment_id = c.payment_id
where c.payment_id is null or g.payment_id is null
