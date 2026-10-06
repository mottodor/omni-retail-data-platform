{{ config(materialized='table') }}

-- Grain: one row per committed, non-delete customer version (SCD Type 2).
-- Half-open validity is expressed in PostgreSQL source LSNs. Source timestamps
-- are retained for audit only and never select version succession.
select
    customer_key,
    customer_id,
    email,
    first_name,
    last_name,
    region,
    city,
    customer_status,
    segment,
    registered_at,
    created_at,
    updated_at,
    valid_from_lsn,
    valid_to_lsn,
    is_snapshot_baseline,
    is_current,
    opened_at_source_timestamp,
    closed_at_source_timestamp,
    event_id,
    source_tx_id,
    kafka_offset
from {{ ref('int_cdc_customer_versions') }}
