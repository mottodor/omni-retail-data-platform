{{ config(materialized='view') }}

-- Grain: one row per OLTP snapshot record (payment_id, _batch_id).
select
    payment_id,
    order_id,
    method,
    status,
    amount,
    transaction_id,
    created_at,
    updated_at,
    _batch_id,
    _batch_date,
    _source_object,
    _ingested_at
from {{ source('bronze', 'payments') }}
