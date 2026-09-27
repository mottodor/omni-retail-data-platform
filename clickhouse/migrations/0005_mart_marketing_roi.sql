-- Serving copy of iceberg.analytics.mart_marketing_roi (Phase 6 slice 2,
-- ADR 0004): full-snapshot staging swap. The _staging twin mirrors the
-- serving DDL exactly (same columns, engine, ORDER BY) — required for
-- EXCHANGE TABLES.
--
-- Physical design (guide §26.2, ratified in slice 2):
-- - MergeTree — swap-based publication removes any need for dedup engines.
-- - ORDER BY (channel, start_date, campaign_id): campaign panels filter
--   by channel and start_date range; campaign_id ties the key to the
--   grain (one row per campaign).
-- - No partitioning: hundreds of rows, a partition key buys nothing.
-- - No TTL: historical portfolio data, expiry has no meaning.
--
-- Nullability mirrors the Gold model semantics: end_date is optional
-- (ongoing campaigns); ctr/cpc_eur/cpm_eur/budget_utilization are
-- nullif-protected divisions (zero impressions/clicks -> NULL, never an
-- error); everything else is NOT NULL by construction.
--
-- Type mapping from the Trino mart: varchar -> String, date -> Date,
-- bigint counts -> UInt64, double -> Float64, decimal(p,s) -> Decimal(p,s)
-- mirrored exactly.

CREATE TABLE IF NOT EXISTS analytics.mart_marketing_roi
(
    campaign_id String,
    campaign_name String,
    channel String,
    campaign_status String,
    start_date Date,
    end_date Nullable(Date),
    budget_eur Decimal(12, 2),
    spend_eur Decimal(12, 2),
    impressions UInt64,
    clicks UInt64,
    ctr Nullable(Float64),
    cpc_eur Nullable(Float64),
    cpm_eur Nullable(Float64),
    budget_utilization Nullable(Decimal(27, 15))
)
ENGINE = MergeTree
ORDER BY (channel, start_date, campaign_id);

CREATE TABLE IF NOT EXISTS analytics.mart_marketing_roi_staging
(
    campaign_id String,
    campaign_name String,
    channel String,
    campaign_status String,
    start_date Date,
    end_date Nullable(Date),
    budget_eur Decimal(12, 2),
    spend_eur Decimal(12, 2),
    impressions UInt64,
    clicks UInt64,
    ctr Nullable(Float64),
    cpc_eur Nullable(Float64),
    cpm_eur Nullable(Float64),
    budget_utilization Nullable(Decimal(27, 15))
)
ENGINE = MergeTree
ORDER BY (channel, start_date, campaign_id);
