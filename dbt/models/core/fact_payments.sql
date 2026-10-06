{{ config(materialized='table') }}

-- Grain: one row per live CDC payment. PK: payment_id.
-- Measures: payment_amount. Degenerate: transaction_id.
-- Reconciled against the live CDC order set by a singular business test.
select
    payment_id,
    order_id,
    method as payment_method,
    status as payment_status,
    amount as payment_amount,
    transaction_id,
    created_at
from {{ ref('int_cdc_payments_current') }}
