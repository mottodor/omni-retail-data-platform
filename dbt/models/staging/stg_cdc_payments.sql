{{ config(materialized='view') }}

-- Grain: one validated CDC event for one payment.
select
    cast(json_extract_scalar(key_json, '$.payment_id') as bigint) as payment_id,
    case when operation <> 'd'
        then cast(json_extract_scalar(after_json, '$.order_id') as bigint)
    end as order_id,
    case when operation <> 'd'
        then json_extract_scalar(after_json, '$.method')
    end as method,
    case when operation <> 'd'
        then json_extract_scalar(after_json, '$.status')
    end as status,
    case when operation <> 'd'
        then cast(json_extract_scalar(after_json, '$.amount') as decimal(12, 2))
    end as amount,
    case when operation <> 'd'
        then json_extract_scalar(after_json, '$.transaction_id')
    end as transaction_id,
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
  and source_table = 'payments'
  and kafka_topic = 'omni.oltp.public.payments'
{{ cdc_boundary_predicate('omni.oltp.public.payments') }}
