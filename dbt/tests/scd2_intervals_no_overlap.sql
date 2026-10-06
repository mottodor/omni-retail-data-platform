-- Customer SCD2 uses half-open PostgreSQL-LSN intervals. The initial
-- snapshot baseline has no valid_from_lsn; streamed versions do. Deletes may
-- create a gap before a later recreate, but emitted intervals must not overlap.
with sequenced as (
    select
        customer_id,
        customer_key,
        valid_from_lsn,
        valid_to_lsn,
        is_snapshot_baseline,
        row_number() over (
            partition by customer_id
            order by
                case when is_snapshot_baseline then 0 else 1 end,
                valid_from_lsn nulls first,
                customer_key
        ) as version_number,
        lead(valid_from_lsn) over (
            partition by customer_id
            order by
                case when is_snapshot_baseline then 0 else 1 end,
                valid_from_lsn nulls first,
                customer_key
        ) as next_valid_from_lsn
    from {{ ref('dim_customer') }}
)
select
    customer_id,
    customer_key,
    valid_from_lsn,
    valid_to_lsn,
    next_valid_from_lsn
from sequenced
where (is_snapshot_baseline and (version_number <> 1 or valid_from_lsn is not null))
   or (not is_snapshot_baseline and valid_from_lsn is null)
   or (valid_to_lsn is not null and valid_from_lsn is not null
       and valid_to_lsn <= valid_from_lsn)
   or (valid_to_lsn is not null and next_valid_from_lsn is not null
       and next_valid_from_lsn < valid_to_lsn)
