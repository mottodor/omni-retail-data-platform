{{ config(materialized='view') }}

-- Grain: one row per campaign (the API returns the full campaign list
-- daily; the latest logical batch wins).
with ranked as (
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
        row_number() over (
            partition by campaign_id
            order by _batch_date desc, _ingested_at desc
        ) as version_rank
    from {{ ref('stg_campaigns') }}
)
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
    clicks
from ranked
where version_rank = 1
