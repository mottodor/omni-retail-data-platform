-- Serving copy of iceberg.analytics.mart_daily_sales (Phase 6 slice 1,
-- ADR 0004): full-snapshot staging swap. The _staging twin mirrors the
-- serving DDL exactly (same columns, engine, ORDER BY) — required for
-- EXCHANGE TABLES and keeps the REPLACE PARTITION option available for
-- slice 3 without migration rework.
--
-- Physical design note (guide §26.2): MergeTree with the mart grain
-- (order_date, category_name, region) as ORDER BY; partitioning and TTL
-- deliberately absent — the per-mart engine design is re-evaluated in
-- Phase 6 slice 2 via a new migration (data is derived; republish only).
--
-- Type mapping from the Trino mart: integer -> Int32, date -> Date,
-- varchar -> String, bigint counts -> UInt64. Money columns mirror the
-- Gold types exactly (decimal division artifacts included: revenue_eur
-- decimal(38,21), margin_eur decimal(38,6)) so values round-trip 1:1;
-- normalizing scales would be a slice 2 decision, taken with a migration.

CREATE TABLE IF NOT EXISTS analytics.mart_daily_sales
(
    date_key Int32,
    order_date Date,
    category_name String,
    region String,
    orders_count UInt64,
    items_sold UInt64,
    revenue_eur Decimal(38, 21),
    margin_eur Decimal(38, 6)
)
ENGINE = MergeTree
ORDER BY (order_date, category_name, region);

CREATE TABLE IF NOT EXISTS analytics.mart_daily_sales_staging
(
    date_key Int32,
    order_date Date,
    category_name String,
    region String,
    orders_count UInt64,
    items_sold UInt64,
    revenue_eur Decimal(38, 21),
    margin_eur Decimal(38, 6)
)
ENGINE = MergeTree
ORDER BY (order_date, category_name, region);
