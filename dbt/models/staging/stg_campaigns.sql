{{ config(materialized='view') }}

-- Grain: one row per campaign per raw API page (daily campaign snapshots).
select
    campaign_id,
    name,
    channel,
    status,
    start_date,
    end_date,
    budget_eur,
    spend_eur,
    impressions,
    clicks,
    _batch_id,
    _batch_date,
    _source_object,
    _ingested_at
from {{ source('bronze', 'campaigns') }}
