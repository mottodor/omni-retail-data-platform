{{ config(materialized='view') }}

-- Grain: one row per shipment (latest known version).
with ranked as (
    select
        shipment_id,
        order_id,
        carrier,
        tracking_number,
        status,
        shipped_at,
        delivered_at,
        created_at,
        updated_at,
        row_number() over (
            partition by shipment_id
            order by _batch_date desc, _ingested_at desc
        ) as version_rank
    from {{ ref('stg_shipments') }}
)
select
    shipment_id,
    order_id,
    carrier,
    tracking_number,
    status,
    shipped_at,
    delivered_at,
    created_at
from ranked
where version_rank = 1
