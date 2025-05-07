{{ config(materialized='table') }}

/*
snowflake.organization_usage.rate_sheet_daily isn't guaranteed to have 1 row per day per usage type.

If you don't consume any compute resources on a given day, there won't be a record.

This model guarantees 1 row per day per usage type, by filling in missing values with rates from the last
known day.
*/

with
dates_base as (
    select date_day as date from (
        {{ dbt_utils.date_spine(
                datepart="day",
                start_date="'2018-01-01'",
                end_date="dateadd(day, 1, current_date)"
            )
        }}
    )
),

rate_sheet_daily_base as (
    select
        organization_name,
        account_name,
        account_locator,
        date,
        usage_type,
        currency,
        effective_rate,
        service_type
    from {{ ref('stg_rate_sheet_daily') }}
    {% if not var('uses_org_view', false) %}
    where
        account_name = {{ account_name() }}
    {% endif %}
),

stop_thresholds as (
    select min(date) as start_date
    from rate_sheet_daily_base

    union all

    select min(date) as start_date
    from {{ ref('remaining_balance_daily_without_contract_view') }}
),

date_range as (
    select
        max(start_date) as start_date,
        current_date as end_date
    from stop_thresholds
),

remaining_balance_daily as (
    select
        date,
        free_usage_balance + capacity_balance + on_demand_consumption_balance + rollover_balance as remaining_balance,
        remaining_balance < 0 as is_account_in_overage
    from {{ ref('remaining_balance_daily_without_contract_view') }}
),

latest_remaining_balance_daily as (
    select
        date,
        remaining_balance,
        is_account_in_overage
    from remaining_balance_daily
    qualify row_number() over (
order by date desc) = 1
),

rate_sheet_daily as (
    select rate_sheet_daily_base.*
    from rate_sheet_daily_base
    inner join date_range
        on rate_sheet_daily_base.date between date_range.start_date and date_range.end_date
),

rates_date_range_w_usage_types as (
    select
        date_range.start_date,
        date_range.end_date,
        usage_types.account_name,
        usage_types.usage_type
    from date_range
    cross join (select distinct rate_sheet_daily.account_name, rate_sheet_daily.usage_type from rate_sheet_daily) as usage_types
),

base as (
    select
        db.date,
        dr.usage_type,
        dr.account_name
    from dates_base as db
    inner join rates_date_range_w_usage_types as dr
        on db.date between dr.start_date and dr.end_date
),

rates_w_overage as (
    select
        base.date,
        base.usage_type,
        base.account_name,
        coalesce(
            rate_sheet_daily.service_type,
            lag(rate_sheet_daily.service_type) ignore nulls over (
                partition by base.usage_type
                , base.account_name
                order by base.date
            ),
            lead(rate_sheet_daily.service_type) ignore nulls over (
                partition by base.usage_type
                , base.account_name
                order by base.date
            )
        ) as service_type,
        coalesce(
            rate_sheet_daily.effective_rate,
            lag(rate_sheet_daily.effective_rate) ignore nulls over (
                partition by base.usage_type
                , base.account_name
                order by base.date
            ),
            lead(rate_sheet_daily.effective_rate) ignore nulls over (
                partition by base.usage_type
                , base.account_name
                order by base.date
            )
        ) as effective_rate,
        coalesce(
            rate_sheet_daily.currency,
            lag(rate_sheet_daily.currency) ignore nulls over (
                partition by base.usage_type
                , base.account_name
                order by base.date
            ),
            lead(rate_sheet_daily.currency) ignore nulls over (
                partition by base.usage_type
                , base.account_name
                order by base.date
            )
        ) as currency,
        base.usage_type like 'overage-%' as is_overage_rate,
        replace(base.usage_type, 'overage-', '') as associated_usage_type,
        coalesce(remaining_balance_daily.is_account_in_overage, latest_remaining_balance_daily.is_account_in_overage, false) as _is_account_in_overage,
        case
            when _is_account_in_overage and is_overage_rate then 1
            when not _is_account_in_overage and not is_overage_rate then 1
            else 0
        end as rate_priority

    from base
    left join latest_remaining_balance_daily on latest_remaining_balance_daily.date is not null
    left join remaining_balance_daily
        on base.date = remaining_balance_daily.date
    left join rate_sheet_daily
        on base.date = rate_sheet_daily.date
            and base.usage_type = rate_sheet_daily.usage_type
            and base.account_name = rate_sheet_daily.account_name
),

rates as (
    select
        date,
        account_name,
        usage_type,
        associated_usage_type,
        service_type,
        effective_rate,
        currency,
        is_overage_rate
    from rates_w_overage
    qualify row_number() over (
        partition by date, service_type, associated_usage_type
        , account_name
        order by rate_priority desc
    ) = 1
)

select
    date,
    account_name,
    associated_usage_type as usage_type,
    service_type,
    effective_rate,
    currency,
    is_overage_rate,
    row_number() over (
        partition by service_type, associated_usage_type
        , account_name
        order by date desc
    ) = 1 as is_latest_rate
from rates
order by date
