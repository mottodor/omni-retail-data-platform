-- Typed models rely on the fixed table-topic route and the one-partition local
-- ordering contract from ADR 0006.
select event_id
from {{ source('bronze', 'postgres_cdc_events') }}
where source_schema <> 'public'
   or kafka_partition <> 0
   or (source_table = 'customers' and kafka_topic <> 'omni.oltp.public.customers')
   or (source_table = 'orders' and kafka_topic <> 'omni.oltp.public.orders')
   or (source_table = 'payments' and kafka_topic <> 'omni.oltp.public.payments')
