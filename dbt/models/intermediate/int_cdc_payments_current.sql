{{ config(materialized='view') }}

-- Grain: one row per live payment business key. Initial r events are the
-- pre-streaming baseline; rank streamed deletes before filtering.
with ranked as (
    select
        *,
        row_number() over (
            partition by payment_id
            order by
                case when operation = 'r' then 0 else 1 end desc,
                source_lsn desc nulls last,
                kafka_offset desc,
                event_id desc
        ) as version_rank
    from {{ ref('stg_cdc_payments') }}
)
select
    payment_id,
    order_id,
    method,
    status,
    amount,
    transaction_id,
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
