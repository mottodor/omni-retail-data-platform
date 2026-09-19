-- FX coverage business test (Phase 5 slice 3, Review Focus 1):
-- every non-EUR order must have a usable rate (latest rate on or before the
-- order date). NULL here means silently un-convertible revenue — fail loud.
select
    fx.order_id,
    fx.currency,
    fx.order_date
from (
    select
        order_id,
        currency,
        date(created_at) as order_date,
        rate_to_eur
    from {{ ref('int_orders_fx') }}
) as fx
where fx.currency <> 'EUR'
  and fx.rate_to_eur is null
