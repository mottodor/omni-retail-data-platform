-- Gold source succession is defined by PostgreSQL LSN. Initial r events are
-- the only events allowed to omit it; a streamed c/u/d without LSN is an
-- incompatible event and must fail the build.
select event_id, 'customers' as source_table, operation
from {{ ref('stg_cdc_customers') }}
where operation <> 'r' and source_lsn is null

union all

select event_id, 'orders' as source_table, operation
from {{ ref('stg_cdc_orders') }}
where operation <> 'r' and source_lsn is null

union all

select event_id, 'payments' as source_table, operation
from {{ ref('stg_cdc_payments') }}
where operation <> 'r' and source_lsn is null
