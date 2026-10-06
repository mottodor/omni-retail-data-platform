{{ config(materialized='table') }}

-- Grain: one row per snapshot shipment whose CDC parent order is live.
-- The parent gate suppresses stale snapshot rows after a cascade delete;
-- children for a new CDC order can remain absent until the next snapshot.
select
    s.shipment_id,
    s.order_id,
    s.carrier,
    s.tracking_number,
    s.status as shipment_status,
    s.shipped_at,
    s.delivered_at,
    s.created_at
from {{ ref('int_shipments') }} as s
join {{ ref('fact_orders') }} as o
    on o.order_id = s.order_id
