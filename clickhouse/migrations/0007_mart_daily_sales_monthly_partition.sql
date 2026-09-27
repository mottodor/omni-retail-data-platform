-- Phase 6 slice 2: re-partition mart_daily_sales to monthly
-- toYYYYMM(order_date) — the single naturally date-partitioned mart
-- (ADR 0004). Functional justification: monthly partitions keep the
-- slice-3 REPLACE PARTITION (incremental publication) option open for
-- the one mart where it could pay off; the swap path stays the fallback.
--
-- Serving data is derived from Iceberg Gold: the physical change is
-- DROP + recreate of the serving/_staging pair — no data migration.
-- REpublish after this migration applies:
--   make serving-rebuild ARGS="--mart mart_daily_sales"
-- (or `make serving-rebuild` for every mart). Migration 0003 stays in
-- the ledger as history; THIS file carries the current DDL that the
-- specs-vs-migration unit test cross-checks.

DROP TABLE IF EXISTS analytics.mart_daily_sales;
DROP TABLE IF EXISTS analytics.mart_daily_sales_staging;

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
ORDER BY (order_date, category_name, region)
PARTITION BY toYYYYMM(order_date);

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
ORDER BY (order_date, category_name, region)
PARTITION BY toYYYYMM(order_date);
