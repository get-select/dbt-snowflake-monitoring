{{ config(materialized='table') }}

with
daily_rates_base as (
    select
        date,
        effective_rate,
        max(date) over() as latest_date
    from {{ ref('rate_sheet_daily') }}
    where usage_type = 'compute'
),

/*
Some days have more than 1 rate, likely due to a new contract. Just pick the higher rate for that day.
*/
daily_rates_deduped as (
    select
        date,
        get(array_agg(effective_rate) within group (order by effective_rate desc), 0) as effective_rate,
        any_value(latest_date) as latest_date
    from daily_rates_base
    group by 1
),

/*
rate_sheet_daily will always be behind by 1 day
to avoid losing queries from today when we inner join, we will
add a new record to rate sheet daily for the missing day, using
the most recent effective_rate
*/
daily_rates as (
    select
        date,
        effective_rate
    from daily_rates_deduped

    union all

    select
        dateadd('day', 1, latest_date) as date,
        effective_rate
    from daily_rates_deduped
    where
        date = latest_date
),
/*
Calculate a "stop threshold", which tells us the latest timestamp we should process data up until.

For example, if warehouse metering history has data up until 2022-10-10 16:00:00, daily_rates
has data for 2022-10-10 (aka the full day), then we would only want to consider queries
that ended before 2022-10-10 16:00:00. Otherwise we won't accurately calculate their cost.
*/
stop_thresholds as (
    select max(end_time) as latest_ts
    from {{ ref('warehouse_metering_history') }}

    union all

    -- Can use data up until the end of the day of the latest date in daily_rates
    select dateadd('day', 1, max(date)) as latest_ts
    from daily_rates
),

stop_threshold as (
    select min(latest_ts) as latest_ts
    from stop_thresholds
),

filtered_queries as (
    select
        query_history.query_id,
        query_history.query_text as original_query_text,
        query_history.credits_used_cloud_services,
        query_history.warehouse_id,
        query_history.warehouse_size is not null as ran_on_warehouse,
        timeadd(
            'millisecond',
            query_history.queued_overload_time + query_history.compilation_time
            + query_history.queued_provisioning_time + query_history.queued_repair_time +
            query_history.list_external_files_time,
            query_history.start_time
        ) as execution_start_time,
        query_history.start_time,
        query_history.end_time
    from {{ ref('query_history') }}
    cross join stop_threshold
    where
        query_history.end_time <= stop_threshold.latest_ts
),

hours_list as (
    select
        dateadd(
            'hour',
            '-' || row_number() over (order by null),
            dateadd('day', '+1', current_date)
        ) as hour_start,
        dateadd('hour', '+1', hour_start) as hour_end
    from table(generator(rowcount => (24 * 730)))
),

-- 1 row per hour a query ran
query_hours as (
    select
        hl.hour_start,
        hl.hour_end,
        queries.*
    from hours_list as hl
    inner join filtered_queries as queries
        on hl.hour_start >= date_trunc('hour', queries.execution_start_time)
            and hl.hour_start < queries.end_time
            and queries.ran_on_warehouse
),

query_seconds_per_hour as (
    select
        *,
        datediff('millisecond', greatest(execution_start_time, hour_start), least(end_time, hour_end)) as num_milliseconds_query_ran,
        sum(num_milliseconds_query_ran) over (partition by warehouse_id, hour_start) as total_query_milliseconds_in_hour,
        num_milliseconds_query_ran / total_query_milliseconds_in_hour as fraction_of_total_query_time_in_hour,
        hour_start as hour
    from query_hours
),

credits_billed_hourly as (
    select
        start_time as hour,
        warehouse_id,
        credits_used_compute,
        credits_used_cloud_services
    from {{ ref('warehouse_metering_history') }}
),

query_cost as (
    select
        query_seconds_per_hour.*,
        credits_billed_hourly.credits_used_compute * daily_rates.effective_rate as actual_warehouse_cost,
        credits_billed_hourly.credits_used_compute * query_seconds_per_hour.fraction_of_total_query_time_in_hour * daily_rates.effective_rate as allocated_compute_cost_in_hour
    from query_seconds_per_hour
    inner join credits_billed_hourly
        on query_seconds_per_hour.warehouse_id = credits_billed_hourly.warehouse_id
            and query_seconds_per_hour.hour = credits_billed_hourly.hour
    inner join daily_rates
        on date(query_seconds_per_hour.start_time) = daily_rates.date
),

cost_per_query as (
    select
        query_id,
        any_value(start_time) as start_time,
        sum(allocated_compute_cost_in_hour) as compute_cost,
        any_value(credits_used_cloud_services) as credits_used_cloud_services
    from query_cost
    group by 1
),

credits_billed_daily as (
    select
        date(hour) as date,
        sum(credits_used_compute) as daily_credits_used_compute,
        sum(credits_used_cloud_services) as daily_credits_used_cloud_services,
        greatest(daily_credits_used_cloud_services - daily_credits_used_compute * 0.1, 0) as daily_billable_cloud_services
    from credits_billed_hourly
    group by 1
),

all_queries as (
    select
        query_id,
        start_time,
        compute_cost,
        credits_used_cloud_services
    from cost_per_query

    union all

    select
        query_id,
        start_time,
        0 as compute_cost,
        credits_used_cloud_services
    from filtered_queries
    where
        not ran_on_warehouse
)

select
    all_queries.query_id,
    all_queries.start_time,
    all_queries.compute_cost,
    (all_queries.credits_used_cloud_services / credits_billed_daily.daily_credits_used_cloud_services * credits_billed_daily.daily_billable_cloud_services) * daily_rates.effective_rate as cloud_services_cost,
    all_queries.compute_cost + cloud_services_cost as query_cost
from all_queries
inner join credits_billed_daily
    on date(all_queries.start_time) = credits_billed_daily.date
inner join daily_rates
    on date(all_queries.start_time) = daily_rates.date
order by all_queries.start_time
