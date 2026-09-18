{{ config(materialized='view') }}

-- Grain: one row per OLTP snapshot record (category_id, _batch_id).
select
    category_id,
    name,
    parent_category_id,
    created_at,
    updated_at,
    _batch_id,
    _batch_date,
    _source_object,
    _ingested_at
from {{ source('bronze', 'categories') }}
