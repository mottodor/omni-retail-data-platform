{{ config(materialized='table') }}

-- Grain: one row per marketing campaign (current API snapshot; SCD Type 1).
-- budget/spend/impressions/clicks are current-snapshot attributes; derived
-- KPIs (CTR/CPC/CPM/ROAS) are computed in mart_marketing_roi (slice 3).
select
    campaign_id,
    name as campaign_name,
    channel,
    status as campaign_status,
    start_date,
    end_date,
    budget_eur,
    spend_eur,
    impressions,
    clicks
from {{ ref('int_campaigns') }}
