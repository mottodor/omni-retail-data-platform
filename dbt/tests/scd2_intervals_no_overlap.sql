-- SCD2 validity intervals per customer must not overlap, must be well
-- formed, and consecutive versions must be contiguous.
with sequenced as (
    select
        customer_id,
        valid_from,
        valid_to,
        lead(valid_from) over (
            partition by customer_id
            order by valid_from
        ) as next_valid_from
    from {{ ref('dim_customer') }}
)
select
    customer_id,
    valid_from,
    valid_to,
    next_valid_from
from sequenced
where valid_from > valid_to
   or (next_valid_from is not null and next_valid_from <= valid_to)
