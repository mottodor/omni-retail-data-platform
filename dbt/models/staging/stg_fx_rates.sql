{{ config(materialized='view') }}

-- Grain: one row per (currency, raw API page) of one logical batch.
select
    currency,
    rate,
    _batch_id,
    _batch_date,
    _source_object,
    _ingested_at
from {{ source('bronze', 'fx_rates') }}
