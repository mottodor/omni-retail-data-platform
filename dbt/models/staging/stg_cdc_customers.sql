{{ config(materialized='view') }}

-- Grain: one validated CDC event for one customer.
-- Deletes intentionally retain only the key and event metadata: PostgreSQL's
-- default replica identity does not guarantee complete old non-key values.
select
    cast(json_extract_scalar(key_json, '$.customer_id') as bigint) as customer_id,
    case when operation <> 'd'
        then json_extract_scalar(after_json, '$.email')
    end as email,
    case when operation <> 'd'
        then json_extract_scalar(after_json, '$.first_name')
    end as first_name,
    case when operation <> 'd'
        then json_extract_scalar(after_json, '$.last_name')
    end as last_name,
    case when operation <> 'd'
        then json_extract_scalar(after_json, '$.region')
    end as region,
    case when operation <> 'd'
        then json_extract_scalar(after_json, '$.city')
    end as city,
    case when operation <> 'd'
        then json_extract_scalar(after_json, '$.status')
    end as status,
    case when operation <> 'd'
        then json_extract_scalar(after_json, '$.segment')
    end as segment,
    case when operation <> 'd'
        then cast(
            from_iso8601_timestamp_nanos(
                json_extract_scalar(after_json, '$.registered_at')
            ) as timestamp(6) with time zone
        )
    end as registered_at,
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
  and source_table = 'customers'
  and kafka_topic = 'omni.oltp.public.customers'
{{ cdc_boundary_predicate('omni.oltp.public.customers') }}
