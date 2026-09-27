-- Serving copy of iceberg.analytics.mart_customer_ltv (Phase 6 slice 2,
-- ADR 0004): full-snapshot staging swap. The _staging twin mirrors the
-- serving DDL exactly (same columns, engine, ORDER BY) — required for
-- EXCHANGE TABLES.
--
-- Physical design (guide §26.2, ratified in slice 2):
-- - MergeTree — swap-based publication removes any need for dedup engines.
-- - ORDER BY (region, segment, customer_id): LTV panels filter by
--   region/segment and then locate customers; the grain column closes the
--   key.
-- - No partitioning: full-history snapshot per customer — first/last order
--   dates are attributes, not filter keys.
-- - No TTL: historical portfolio data, expiry has no meaning.
--
-- Nullability mirrors the Gold model semantics: Nullable(..) only where
-- the dbt model can produce NULLs (case-without-else, nullif, optional
-- source attributes); everything else is NOT NULL by construction — an
-- unexpected NULL fails the staging insert loudly instead of corrupting
-- the serving copy. avg_order_value_eur is NULL when the customer has no
-- realized orders (case-without-else in the mart).
--
-- Type mapping from the Trino mart: bigint identifier -> Int64,
-- varchar -> String, date -> Date, bigint counts -> UInt64. gmv_eur and
-- avg_order_value_eur mirror the Gold types exactly (decimal(38,21)
-- division artifacts included) so values round-trip 1:1 — normalizing
-- the scales is deliberately rejected (slice 2 decision): exact mirroring
-- keeps Gold-vs-CH reconciliation trivially provable.

CREATE TABLE IF NOT EXISTS analytics.mart_customer_ltv
(
    customer_id Int64,
    customer_key String,
    region String,
    segment String,
    first_order_date Date,
    last_order_date Date,
    orders_count UInt64,
    gmv_eur Decimal(38, 21),
    avg_order_value_eur Nullable(Decimal(38, 21))
)
ENGINE = MergeTree
ORDER BY (region, segment, customer_id);

CREATE TABLE IF NOT EXISTS analytics.mart_customer_ltv_staging
(
    customer_id Int64,
    customer_key String,
    region String,
    segment String,
    first_order_date Date,
    last_order_date Date,
    orders_count UInt64,
    gmv_eur Decimal(38, 21),
    avg_order_value_eur Nullable(Decimal(38, 21))
)
ENGINE = MergeTree
ORDER BY (region, segment, customer_id);
