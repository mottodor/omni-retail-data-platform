-- A live CDC customer has exactly one open version; a deleted customer has
-- zero. No closed version may be marked current, and no current dimension row
-- may be absent from the delete-aware current-state model.
with dimension_counts as (
    select
        customer_id,
        count_if(is_current) as current_count,
        count_if(is_current and valid_to_lsn is not null) as closed_current_count,
        count_if(not is_current and valid_to_lsn is null) as open_old_count
    from {{ ref('dim_customer') }}
    group by customer_id
),
live_counts as (
    select
        c.customer_id,
        count(d.customer_key) as dimension_current_count
    from {{ ref('int_cdc_customers_current') }} as c
    left join {{ ref('dim_customer') }} as d
        on d.customer_id = c.customer_id
       and d.is_current
    group by c.customer_id
),
invalid_dimension_counts as (
    select
        customer_id,
        'invalid_dimension_flags' as failure_reason
    from dimension_counts
    where current_count > 1
       or closed_current_count > 0
       or open_old_count > 0
),
invalid_live_counts as (
    select
        customer_id,
        'live_customer_current_count' as failure_reason
    from live_counts
    where dimension_current_count <> 1
),
current_without_live_customer as (
    select
        d.customer_id,
        'current_version_without_live_customer' as failure_reason
    from {{ ref('dim_customer') }} as d
    left join {{ ref('int_cdc_customers_current') }} as c
        on c.customer_id = d.customer_id
    where d.is_current
      and c.customer_id is null
)
select * from invalid_dimension_counts
union all
select * from invalid_live_counts
union all
select * from current_without_live_customer
