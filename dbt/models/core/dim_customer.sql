{{ config(materialized='table') }}

-- Grain: one row per customer version (SCD Type 2, daily version grain).
-- valid_from  = logical batch date (_batch_date) that first carried the
--               version; incremental batches contain only changed rows, so
--               every bronze row of a customer IS a distinct version;
-- valid_to    = day before the next version's batch date (inclusive
--               [valid_from, valid_to] intervals, overlap-tested);
-- is_current  = the open-ended version (valid_to = 9999-12-31).
-- Multiple source changes within one day collapse into that day's batch
-- (re-runs replace the partition) — documented daily-grain limitation.
with batch_versions as (
    select
        customer_id,
        email,
        first_name,
        last_name,
        region,
        city,
        status,
        segment,
        registered_at,
        _batch_date as valid_from,
        row_number() over (
            partition by customer_id, _batch_date
            order by _ingested_at desc
        ) as batch_rank
    from {{ ref('stg_customers') }}
),
sequenced as (
    select
        customer_id,
        email,
        first_name,
        last_name,
        region,
        city,
        status,
        segment,
        registered_at,
        valid_from,
        lead(valid_from) over (
            partition by customer_id
            order by valid_from
        ) as next_valid_from
    from batch_versions
    where batch_rank = 1
)
select
    cast(customer_id as varchar) || '_' || substr(cast(valid_from as varchar), 1, 10)
        as customer_key,
    customer_id,
    email,
    first_name,
    last_name,
    region,
    city,
    status as customer_status,
    segment,
    registered_at,
    valid_from,
    coalesce(
        cast(next_valid_from - interval '1' day as date),
        date '9999-12-31'
    ) as valid_to,
    next_valid_from is null as is_current
from sequenced
