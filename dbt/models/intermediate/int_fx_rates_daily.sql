{{ config(materialized='view') }}

-- Grain: one row per (currency, calendar day): the latest page loaded for
-- that day wins (re-runs of a logical date replace the bronze partition).
with ranked as (
    select
        currency,
        _batch_date,
        rate,
        row_number() over (
            partition by currency, _batch_date
            order by _ingested_at desc
        ) as version_rank
    from {{ ref('stg_fx_rates') }}
)
select
    currency,
    _batch_date as rate_date,
    rate
from ranked
where version_rank = 1
