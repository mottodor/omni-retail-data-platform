-- Every upsert/read event must project all required source columns. Deletes
-- intentionally retain only the business key and event metadata.
select event_id, 'customers' as source_table
from {{ ref('stg_cdc_customers') }}
where operation <> 'd'
  and (
      email is null
      or first_name is null
      or last_name is null
      or region is null
      or city is null
      or status is null
      or segment is null
      or registered_at is null
      or created_at is null
      or updated_at is null
  )

union all

select event_id, 'orders' as source_table
from {{ ref('stg_cdc_orders') }}
where operation <> 'd'
  and (
      customer_id is null
      or status is null
      or currency is null
      or shipping_cost is null
      or order_total is null
      or created_at is null
      or updated_at is null
  )

union all

select event_id, 'payments' as source_table
from {{ ref('stg_cdc_payments') }}
where operation <> 'd'
  and (
      order_id is null
      or method is null
      or status is null
      or amount is null
      or transaction_id is null
      or created_at is null
      or updated_at is null
  )
