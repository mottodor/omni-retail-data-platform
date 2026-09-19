{{ config(materialized='table') }}

-- Grain: one row per shipment. PK: shipment_id.
-- Degenerate: tracking_number. Temporal ordering is business-tested.
select
    shipment_id,
    order_id,
    carrier,
    tracking_number,
    status as shipment_status,
    shipped_at,
    delivered_at,
    created_at
from {{ ref('int_shipments') }}
