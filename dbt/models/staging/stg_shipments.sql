{{ config(materialized='view') }}

-- Grain: one row per OLTP snapshot record (shipment_id, _batch_id).
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
    _batch_id,
    _batch_date,
    _source_object,
    _ingested_at
from {{ source('bronze', 'shipments') }}
