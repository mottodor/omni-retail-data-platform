-- Status columns must sum to delivery_count so a NEW delivery-API status
-- cannot disappear silently (a group-by would hide it; fixed columns make
-- the gap observable). Transit cannot be negative.
select
    carrier,
    delivery_count,
    delivered_count + in_transit_count + delayed_count + returned_count
        as status_sum,
    avg_transit_hours
from {{ ref('mart_delivery_performance') }}
where delivered_count + in_transit_count + delayed_count + returned_count
        <> delivery_count
   or avg_transit_hours < 0
