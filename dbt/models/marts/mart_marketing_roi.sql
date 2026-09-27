{{ config(materialized='table') }}

-- Grain: one row per marketing campaign (current API snapshot).
-- Derived media KPIs are null-safe: scheduled/day-0 campaigns have zero
-- impressions and clicks (division yields NULL, never an error). No ROAS:
-- revenue attribution does not exist in this data (spec §2) — see
-- schema.yml limitation note.
select
    campaign_id,
    campaign_name,
    channel,
    campaign_status,
    start_date,
    end_date,
    budget_eur,
    spend_eur,
    impressions,
    clicks,
    cast(clicks as double) / nullif(impressions, 0) as ctr,
    spend_eur / nullif(cast(clicks as double), 0) as cpc_eur,
    spend_eur * 1000.0 / nullif(cast(impressions as double), 0) as cpm_eur,
    spend_eur / nullif(budget_eur, 0) as budget_utilization
from {{ ref('dim_campaign') }}
