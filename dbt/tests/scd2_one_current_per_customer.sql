-- Exactly one open (is_current) version per customer, and no closed
-- version may use the open-ended valid_to sentinel.
with per_customer as (
    select
        customer_id,
        count_if(is_current) as current_count,
        count_if(not is_current and valid_to = date '9999-12-31') as open_old_count
    from {{ ref('dim_customer') }}
    group by customer_id
)
select
    customer_id,
    current_count,
    open_old_count
from per_customer
where current_count <> 1
   or open_old_count <> 0
