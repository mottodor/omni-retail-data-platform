{{ config(materialized='table') }}

-- Grain: one row per live order. Mutable attributes come from the winning CDC
-- state, while customer_key is frozen at the current order lifecycle's r/c
-- boundary. Kafka offsets are used only inside the order topic; the
-- cross-entity point-in-time join compares PostgreSQL source LSNs.
with boundary_events as (
    select
        *,
        operation = 'r' as is_snapshot_baseline,
        count_if(operation = 'c') over (
            partition by
                order_id,
                operation = 'r',
                case when operation = 'r' then null else source_lsn end
        ) as create_events_at_boundary,
        row_number() over (
            partition by
                order_id,
                operation = 'r',
                case when operation = 'r' then null else source_lsn end
            order by kafka_offset desc, event_id desc
        ) as boundary_rank
    from {{ ref('stg_cdc_orders') }}
),
lifecycle_candidates as (
    select
        order_id,
        operation as lifecycle_operation,
        source_lsn as lifecycle_start_lsn,
        is_snapshot_baseline,
        kafka_offset,
        event_id,
        row_number() over (
            partition by order_id
            order by
                case when is_snapshot_baseline then 0 else 1 end desc,
                source_lsn desc nulls last,
                kafka_offset desc,
                event_id desc
        ) as lifecycle_rank
    from boundary_events
    where boundary_rank = 1
      and operation <> 'd'
      and (is_snapshot_baseline or create_events_at_boundary > 0)
),
current_lifecycles as (
    select
        order_id,
        lifecycle_operation,
        lifecycle_start_lsn,
        is_snapshot_baseline
    from lifecycle_candidates
    where lifecycle_rank = 1
),
resolved as (
    select
        o.order_id,
        d.customer_key,
        o.customer_id,
        o.status as order_status,
        o.currency,
        o.shipping_cost,
        o.order_total,
        o.created_at,
        o.updated_at
    from {{ ref('int_cdc_orders_current') }} as o
    left join current_lifecycles as l
        on l.order_id = o.order_id
    left join {{ ref('dim_customer') }} as d
        on d.customer_id = o.customer_id
       and (
            (l.is_snapshot_baseline and d.is_snapshot_baseline)
            or (
                not l.is_snapshot_baseline
                and (
                    (
                        d.is_snapshot_baseline
                        and (
                            d.valid_to_lsn is null
                            or l.lifecycle_start_lsn < d.valid_to_lsn
                        )
                    )
                    or (
                        not d.is_snapshot_baseline
                        and d.valid_from_lsn <= l.lifecycle_start_lsn
                        and (
                            d.valid_to_lsn is null
                            or l.lifecycle_start_lsn < d.valid_to_lsn
                        )
                    )
                )
            )
       )
)
select
    order_id,
    customer_key,
    customer_id,
    order_status,
    currency,
    shipping_cost,
    order_total,
    created_at,
    updated_at
from resolved
