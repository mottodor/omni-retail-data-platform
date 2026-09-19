{{ config(materialized='view') }}

-- Grain: one row per category (latest known version).
-- Hard deletes are invisible to snapshot extraction until CDC (Phase 8).
with ranked as (
    select
        category_id,
        name,
        parent_category_id,
        created_at,
        updated_at,
        row_number() over (
            partition by category_id
            order by _batch_date desc, _ingested_at desc
        ) as version_rank
    from {{ ref('stg_categories') }}
)
select
    category_id,
    name,
    parent_category_id,
    created_at,
    updated_at
from ranked
where version_rank = 1
