{{ config(materialized='view') }}

-- Grain: one row per live customer business key.
-- Initial r events are the pre-streaming baseline. Streamed state order is
-- PostgreSQL LSN, then the single-partition table-topic offset. Event/row
-- timestamps and ingestion time are deliberately not ordering keys.
with ranked as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by
                case when operation = 'r' then 0 else 1 end desc,
                source_lsn desc nulls last,
                kafka_offset desc,
                event_id desc
        ) as version_rank
    from {{ ref('stg_cdc_customers') }}
)
select
    customer_id,
    email,
    first_name,
    last_name,
    region,
    city,
    status,
    segment,
    registered_at,
    created_at,
    updated_at,
    event_id,
    operation,
    source_lsn,
    source_tx_id,
    source_timestamp,
    kafka_offset,
    ingested_at
from ranked
where version_rank = 1
  and not is_deleted
