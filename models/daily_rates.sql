{{ config(materialized='table') }}

/*
snowflake.organization_usage.rate_sheet_daily isn't guaranteed to have 1 row per day per usage type.

If you don't consume any compute resources on a given day, there won't be a record.

This model guarantees 1 row per day per usage type, by filling in missing values with rates from the last
known day.
*/

{%- set account_locator -%}
{%- if var('account_locator', none) -%}
'{{ var('account_locator') }}'
{%- else -%}
current_account()
{%- endif -%}
{%- endset -%}

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
        account_locator = {{ account_locator }}
),

remaining_balance_daily as (
    select
        date,
        free_usage_balance + capacity_balance + on_demand_consumption_balance + rollover_balance as remaining_balance,
        remaining_balance < 0 as is_account_in_overage
    from {{ ref('stg_remaining_balance_daily') }}
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

rates_w_overage as (
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
        ) as effective_rate,
        base.usage_type like 'overage-%' as is_overage_rate,
        replace(base.usage_type, 'overage-', '') as associated_usage_type,
        case
            when remaining_balance_daily.is_account_in_overage and is_overage_rate then 1
            when not remaining_balance_daily.is_account_in_overage and not is_overage_rate then 1
            else 0
        end as rate_priority

    from base
    inner join remaining_balance_daily
        on base.date = remaining_balance_daily.date
    left join rate_sheet_daily
        on base.date = rate_sheet_daily.date
            and base.usage_type = rate_sheet_daily.usage_type
),

rates as (
    select
        date,
        usage_type,
        associated_usage_type,
        service_type,
        effective_rate,
        is_overage_rate
    from rates_w_overage
    qualify row_number() over (partition by date, service_type, associated_usage_type order by rate_priority desc) = 1
)

select
    date,
    associated_usage_type as usage_type,
    service_type,
    effective_rate,
    is_overage_rate,
    row_number() over (partition by service_type, associated_usage_type order by date desc) = 1 as is_latest_rate
from rates
order by date
