-- Media-metric validity (Review Focus 2): clicks cannot exceed impressions;
-- ctr must be a rate in (0, 1]; spend cannot be negative. Zero-impression
-- campaigns (scheduled, day 0) legitimately carry NULL ctr/cpc/cpm and are
-- excluded here — the not_null tests in schema.yml are where-scoped.
select
    campaign_id,
    clicks,
    impressions,
    ctr,
    spend_eur
from {{ ref('mart_marketing_roi') }}
where clicks > impressions
   or clicks < 0
   or spend_eur < 0
   or (ctr is not null and (ctr <= 0 or ctr > 1))
