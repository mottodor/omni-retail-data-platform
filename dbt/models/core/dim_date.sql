{{ config(materialized='table') }}

-- Grain: one row per calendar day covering the live CDC order-activity
-- window (created_at UNION updated_at). Empty upstream yields an empty
-- dimension because sequence(NULL, NULL, ...) produces no retained dates.
with activity as (
    select date(created_at) as activity_date
    from {{ ref('fact_orders') }}
    union all
    select date(updated_at) as activity_date
    from {{ ref('fact_orders') }}
),
bounds as (
    select
        min(activity_date) as min_date,
        max(activity_date) as max_date
    from activity
),
calendar as (
    select cast(day as date) as full_date
    from bounds
    cross join unnest(sequence(min_date, max_date, interval '1' day)) as days(day)
)
select
    cast(date_format(cast(full_date as timestamp), '%Y%m%d') as integer) as date_key,
    full_date,
    year(full_date) as year_number,
    quarter(full_date) as quarter_number,
    month(full_date) as month_number,
    date_format(cast(full_date as timestamp), '%M') as month_name,
    day(full_date) as day_number,
    dow(full_date) as day_of_week,
    date_format(cast(full_date as timestamp), '%W') as day_name,
    dow(full_date) in (6, 7) as is_weekend
from calendar
where full_date is not null
