{{ config(materialized='view') }}

-- Grain: one row per OLTP snapshot record (customer_id, _batch_id).
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
    _batch_id,
    _batch_date,
    _source_object,
    _ingested_at
from {{ source('bronze', 'customers') }}
