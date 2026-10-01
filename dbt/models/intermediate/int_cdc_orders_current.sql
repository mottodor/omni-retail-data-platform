{{ config(materialized='view') }}

-- Grain: one row per live order business key. Rank deletes before filtering.
with ranked as (
    select
        *,
        row_number() over (
            partition by order_id
            order by source_lsn desc nulls last, kafka_offset desc, event_id desc
        ) as version_rank
    from {{ ref('stg_cdc_orders') }}
)
select
    order_id,
    customer_id,
    status,
    currency,
    shipping_cost,
    order_total,
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
