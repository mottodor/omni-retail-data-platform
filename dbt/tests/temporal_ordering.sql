-- Event times must be internally ordered (generator invariants).
select
    'fact_shipments' as model_name,
    cast(shipment_id as varchar) as row_key
from {{ ref('fact_shipments') }}
where delivered_at is not null and shipped_at > delivered_at
union all
select
    'fact_orders',
    cast(order_id as varchar)
from {{ ref('fact_orders') }}
where created_at > updated_at
union all
select
    'fact_payments',
    cast(p.payment_id as varchar)
from {{ ref('fact_payments') }} as p
join {{ ref('fact_orders') }} as o
    on o.order_id = p.order_id
where p.created_at < o.created_at
union all
select
    'dim_campaign',
    campaign_id
from {{ ref('dim_campaign') }}
where end_date < start_date
