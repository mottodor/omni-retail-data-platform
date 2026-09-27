-- Serving copy of iceberg.analytics.mart_delivery_performance
-- (Phase 6 slice 2, ADR 0004): full-snapshot staging swap. The _staging
-- twin mirrors the serving DDL exactly (same columns, engine, ORDER BY)
-- — required for EXCHANGE TABLES.
--
-- Physical design (guide §26.2, ratified in slice 2):
-- - MergeTree — swap-based publication removes any need for dedup engines.
-- - ORDER BY (carrier): one row per carrier — the grain column is also
--   the only access key.
-- - No partitioning: dozens of rows, a partition key buys nothing.
-- - No TTL: historical portfolio data, expiry has no meaning.
--
-- Nullability mirrors the Gold model semantics: avg_transit_hours is a
-- case-without-else (NULL until the carrier has a completed delivery);
-- counts and last_update_at are NOT NULL by construction.
--
-- Type mapping from the Trino mart: varchar -> String, bigint counts ->
-- UInt64, double -> Float64, timestamp(6) with time zone ->
-- DateTime64(6, 'UTC') — explicit UTC so reads are deterministic
-- regardless of the server timezone.

CREATE TABLE IF NOT EXISTS analytics.mart_delivery_performance
(
    carrier String,
    delivery_count UInt64,
    delivered_count UInt64,
    in_transit_count UInt64,
    delayed_count UInt64,
    returned_count UInt64,
    avg_transit_hours Nullable(Float64),
    last_update_at DateTime64(6, 'UTC')
)
ENGINE = MergeTree
ORDER BY (carrier);

CREATE TABLE IF NOT EXISTS analytics.mart_delivery_performance_staging
(
    carrier String,
    delivery_count UInt64,
    delivered_count UInt64,
    in_transit_count UInt64,
    delayed_count UInt64,
    returned_count UInt64,
    avg_transit_hours Nullable(Float64),
    last_update_at DateTime64(6, 'UTC')
)
ENGINE = MergeTree
ORDER BY (carrier);
