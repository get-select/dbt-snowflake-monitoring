{{ config(materialized='table') }}

{%- call statement('current_account', fetch_result=True) -%}
      select current_account();
{%- endcall -%}

{%- set current_account = load_result('current_account')['data'][0][0] -%}

/*
snowflake.organization_usage.rate_sheet_daily isn't guaranteed to have 1 row per day per usage type.

If you don't consume any compute resources on a given day, there won't be a record.

This model guarantees 1 row per day per usage type, by filling in missing values with rates from the last
known day.
*/

with
dates_base as (
    select dateadd(
            'day',
            '-' || row_number() over (order by null),
            dateadd('day', '+1', current_date)
        ) as date
    from table(generator(rowcount => (365 * 3)))
),

rate_sheet_daily as (
    select
        date,
        usage_type,
        currency,
        effective_rate,
        service_type
    from {{ ref('stg_rate_sheet_daily') }}
    where
        account_locator = '{{ current_account }}'
),

rates_date_range as (
    select
        min(date) as start_date,
        max(date) as end_date
    from rate_sheet_daily
),

rates_date_range_w_usage_types as (
    select
        rates_date_range.start_date,
        rates_date_range.end_date,
        usage_types.usage_type
    from rates_date_range
    cross join (select distinct usage_type from rate_sheet_daily) as usage_types
),

base as (
    select
        db.date,
        dr.usage_type
    from dates_base as db
    inner join rates_date_range_w_usage_types as dr
        on db.date between dr.start_date and dr.end_date
),

rates as (
    select
        base.date,
        base.usage_type,
        coalesce(
            rate_sheet_daily.service_type,
            lag(rate_sheet_daily.service_type) ignore nulls over (partition by base.usage_type order by base.date),
            lead(rate_sheet_daily.service_type) ignore nulls over (partition by base.usage_type order by base.date)
        ) as service_type,
        coalesce(
            rate_sheet_daily.effective_rate,
            lag(rate_sheet_daily.effective_rate) ignore nulls over (partition by base.usage_type order by base.date),
            lead(rate_sheet_daily.effective_rate) ignore nulls over (partition by base.usage_type order by base.date)
        ) as effective_rate
    from base
    left join rate_sheet_daily
        on base.date = rate_sheet_daily.date
            and base.usage_type = rate_sheet_daily.usage_type
)

select *
from rates
order by date


