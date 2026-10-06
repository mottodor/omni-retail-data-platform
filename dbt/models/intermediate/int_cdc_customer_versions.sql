{{ config(materialized='view') }}

-- Grain: one row per committed, non-delete customer version.
-- Initial Debezium reads are a pre-streaming baseline even when they carry an
-- LSN. Streaming events are collapsed to the final customer-topic offset at
-- each PostgreSQL LSN before half-open validity intervals are constructed.
with boundary_ranked as (
    select
        *,
        operation = 'r' as is_snapshot_baseline,
        row_number() over (
            partition by
                customer_id,
                operation = 'r',
                case when operation = 'r' then null else source_lsn end
            order by kafka_offset desc, event_id desc
        ) as boundary_rank
    from {{ ref('stg_cdc_customers') }}
),
boundary_winners as (
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
        operation,
        event_id,
        source_lsn,
        source_tx_id,
        source_timestamp,
        kafka_topic,
        kafka_partition,
        kafka_offset,
        kafka_timestamp,
        ingested_at,
        is_snapshot_baseline
    from boundary_ranked
    where boundary_rank = 1
),
sequenced as (
    select
        *,
        lead(source_lsn) over (
            partition by customer_id
            order by
                case when is_snapshot_baseline then 0 else 1 end,
                source_lsn nulls first,
                kafka_offset,
                event_id
        ) as next_source_lsn,
        lead(source_timestamp) over (
            partition by customer_id
            order by
                case when is_snapshot_baseline then 0 else 1 end,
                source_lsn nulls first,
                kafka_offset,
                event_id
        ) as next_source_timestamp,
        lead(event_id) over (
            partition by customer_id
            order by
                case when is_snapshot_baseline then 0 else 1 end,
                source_lsn nulls first,
                kafka_offset,
                event_id
        ) as next_event_id
    from boundary_winners
)
select
    cast(customer_id as varchar) || '_' || event_id as customer_key,
    customer_id,
    email,
    first_name,
    last_name,
    region,
    city,
    status as customer_status,
    segment,
    registered_at,
    created_at,
    updated_at,
    case when is_snapshot_baseline then null else source_lsn end as valid_from_lsn,
    next_source_lsn as valid_to_lsn,
    is_snapshot_baseline,
    next_event_id is null as is_current,
    source_timestamp as opened_at_source_timestamp,
    next_source_timestamp as closed_at_source_timestamp,
    event_id,
    operation,
    source_tx_id,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    ingested_at
from sequenced
where operation <> 'd'
