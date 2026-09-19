{{ config(materialized='view') }}

-- Grain: one row per customer (latest known version).
with ranked as (
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
        created_at,
        updated_at,
        row_number() over (
            partition by customer_id
            order by _batch_date desc, _ingested_at desc
        ) as version_rank
    from {{ ref('stg_customers') }}
)
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
    created_at,
    updated_at
from ranked
where version_rank = 1
