-- Financial measures must be non-negative; FX rates strictly positive.
select
    'fact_orders' as model_name,
    cast(order_id as varchar) as row_key
from {{ ref('fact_orders') }}
where order_total < 0 or shipping_cost < 0
union all
select
    'fact_order_items',
    cast(order_item_id as varchar)
from {{ ref('fact_order_items') }}
where quantity < 0 or unit_price < 0 or line_total < 0
union all
select
    'fact_payments',
    cast(payment_id as varchar)
from {{ ref('fact_payments') }}
where payment_amount < 0
union all
select
    'dim_campaign',
    campaign_id
from {{ ref('dim_campaign') }}
where budget_eur < 0 or spend_eur < 0 or impressions < 0 or clicks < 0
union all
select
    'int_fx_rates_daily',
    currency || '_' || substr(cast(rate_date as varchar), 1, 10)
from {{ ref('int_fx_rates_daily') }}
where rate <= 0
