{{ config(materialized='table') }}

-- Grain: one row per payment. PK: payment_id.
-- Measures: payment_amount. Degenerate: transaction_id.
-- Reconciled against fact_orders by a singular business test.
select
    payment_id,
    order_id,
    method as payment_method,
    status as payment_status,
    amount as payment_amount,
    transaction_id,
    created_at
from {{ ref('int_payments') }}
