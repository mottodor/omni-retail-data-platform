-- Line arithmetic invariant: line_total = quantity * unit_price.
select
    order_item_id
from {{ ref('fact_order_items') }}
where line_total <> quantity * unit_price
