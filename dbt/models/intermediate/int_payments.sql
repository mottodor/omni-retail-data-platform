{{ config(materialized='view') }}

-- Grain: one row per payment (latest known version; status changes over time).
with ranked as (
    select
        payment_id,
        order_id,
        method,
        status,
        amount,
        transaction_id,
        created_at,
        updated_at,
        row_number() over (
            partition by payment_id
            order by _batch_date desc, _ingested_at desc
        ) as version_rank
    from {{ ref('stg_payments') }}
)
select
    payment_id,
    order_id,
    method,
    status,
    amount,
    transaction_id,
    created_at
from ranked
where version_rank = 1
