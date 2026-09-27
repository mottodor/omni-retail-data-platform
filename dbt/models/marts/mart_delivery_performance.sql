{{ config(materialized='table') }}

-- Grain: one row per carrier (delivery-partner API feed; synthetic order
-- references — never joined to OLTP orders, see schema.yml). Fixed status
-- columns follow the API status domain; the counts-sum business test makes
-- an unknown new status loud instead of silently dropped.
with deliveries as (
    select
        carrier,
        status,
        updated_at,
        case
            when shipped_at is not null and delivered_at is not null
                then date_diff('minute', shipped_at, delivered_at)
        end as transit_minutes
    from {{ ref('int_deliveries') }}
)
select
    carrier,
    count(*) as delivery_count,
    count(*) filter (where status = 'delivered') as delivered_count,
    count(*) filter (where status = 'in_transit') as in_transit_count,
    count(*) filter (where status = 'delayed') as delayed_count,
    count(*) filter (where status = 'returned') as returned_count,
    avg(transit_minutes) / 60.0 as avg_transit_hours,
    max(updated_at) as last_update_at
from deliveries
group by carrier
