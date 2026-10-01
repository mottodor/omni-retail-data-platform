{{ config(materialized='view') }}

-- Grain: one validated CDC event for one order.
select
    cast(json_extract_scalar(key_json, '$.order_id') as bigint) as order_id,
    case when operation <> 'd'
        then cast(json_extract_scalar(after_json, '$.customer_id') as bigint)
    end as customer_id,
    case when operation <> 'd'
        then json_extract_scalar(after_json, '$.status')
    end as status,
    case when operation <> 'd'
        then json_extract_scalar(after_json, '$.currency')
    end as currency,
    case when operation <> 'd'
        then cast(json_extract_scalar(after_json, '$.shipping_cost') as decimal(12, 2))
    end as shipping_cost,
    case when operation <> 'd'
        then cast(json_extract_scalar(after_json, '$.order_total') as decimal(12, 2))
    end as order_total,
    case when operation <> 'd'
        then cast(
            from_iso8601_timestamp_nanos(
                json_extract_scalar(after_json, '$.created_at')
            ) as timestamp(6) with time zone
        )
    end as created_at,
    case when operation <> 'd'
        then cast(
            from_iso8601_timestamp_nanos(
                json_extract_scalar(after_json, '$.updated_at')
            ) as timestamp(6) with time zone
        )
    end as updated_at,
    operation,
    operation = 'd' as is_deleted,
    event_id,
    source_lsn,
    source_tx_id,
    source_timestamp,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    ingested_at
from {{ source('bronze', 'postgres_cdc_events') }}
where source_schema = 'public'
  and source_table = 'orders'
  and kafka_topic = 'omni.oltp.public.orders'
