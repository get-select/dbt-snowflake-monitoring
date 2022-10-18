{{ config(materialized='table') }}

with date_spine as (
{{ dbt_utils.date_spine(
        datepart="day",
        start_date="'2019-01-01'::date",
        end_date="current_date"
    )
}}
),

dates as (
    select
        date_day as date,
        day(last_day(date_day)) as days_in_month
    from date_spine
),

storage_terabytes_daily as (
    select
        usage_date,
        'Table and Time Travel' as storage_type,
        database_name,
        sum(average_database_bytes) / power(1024, 4) as storage_terabytes
    from snowflake.account_usage.database_storage_usage_history
    group by 1, 2, 3
    union all
    select
        usage_date,
        'Failsafe' as storage_type,
        database_name,
        sum(average_failsafe_bytes) / power(1024, 4) as storage_terabytes
    from snowflake.account_usage.database_storage_usage_history
    group by 1, 2, 3
    union all
    select
        usage_date,
        'Stage' as storage_type,
        null as database_name,
        sum(average_stage_bytes) / power(1024, 4) as storage_terabytes
    from snowflake.account_usage.stage_storage_usage_history
    group by 1, 2, 3
)

, storage_spend_daily as (
    select
        storage_terabytes_daily.usage_date,
        'Storage' as service,
        storage_terabytes_daily.storage_type,
        null as warehouse_name,
        storage_terabytes_daily.database_name,
        coalesce(storage_terabytes_daily.storage_terabytes / dates.days_in_month * daily_rates.effective_rate, 0) as spend
    from dates
    left join storage_terabytes_daily on dates.date = storage_terabytes_daily.usage_date
    left join {{ ref('daily_rates') }} on
        storage_terabytes_daily.usage_date = daily_rates.date
        and daily_rates.usage_type = 'storage'
),

compute_spend_daily as (
    select
        dates.date,
        'Compute' as service,
        null as storage_type,
        metering_history.name as warehouse_name,
        null as database_name,
        sum(metering_history.credits_used_compute * daily_rates.effective_rate) as spend
    from dates
    left join snowflake.account_usage.metering_history on
        dates.date = convert_timezone('UTC', metering_history.start_time)::date
    left join {{ ref('daily_rates') }} on
        dates.date = daily_rates.date
        and daily_rates.usage_type = 'compute'
        and daily_rates.service_type = 'COMPUTE'
    where metering_history.service_type = 'WAREHOUSE_METERING'
    group by 1, 2, 3, 4
),

adj_for_incl_cloud_services_daily as (
    select
        dates.date,
        'Adj For Incl Cloud Services' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        sum(metering_daily_history.credits_adjustment_cloud_services * daily_rates.effective_rate) as spend
    from dates
    left join snowflake.account_usage.metering_daily_history on
        dates.date = metering_daily_history.usage_date
    left join {{ ref('daily_rates') }} on
        dates.date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    group by 1, 2, 3, 4
),

cloud_services_spend_daily as (
    select
        dates.date,
        'Cloud Services' as service,
        null as storage_type,
        case when metering_history.name = 'CLOUD_SERVICES_ONLY' then 'Cloud Services Only' else metering_history.name end as warehouse_name,
        null as database_name,
        sum(metering_history.credits_used_cloud_services * daily_rates.effective_rate) as spend
    from dates
    left join snowflake.account_usage.metering_history on
        dates.date = convert_timezone('UTC', metering_history.start_time)::date
    left join {{ ref('daily_rates') }} on
        dates.date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    where metering_history.service_type = 'WAREHOUSE_METERING'
    group by 1, 2, 3, 4
),

automatic_clustering_spend_daily as (
    select
        dates.date,
        'Automatic Clustering' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        sum(metering_history.credits_used * daily_rates.effective_rate) as spend
    from dates
    left join snowflake.account_usage.metering_history on
        dates.date = convert_timezone('UTC', metering_history.start_time)::date
    left join {{ ref('daily_rates') }} on
        dates.date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    where metering_history.service_type = 'AUTO_CLUSTERING'
    group by 1, 2, 3, 4
),

materialized_view_spend_daily as (
    select
        dates.date,
        'Materialized Views' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        sum(metering_history.credits_used * daily_rates.effective_rate) as spend
    from dates
    left join snowflake.account_usage.metering_history on
        dates.date = convert_timezone('UTC', metering_history.start_time)::date
    left join {{ ref('daily_rates') }} on
        dates.date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    where metering_history.service_type = 'MATERIALIZED_VIEW'
    group by 1, 2, 3, 4
),

snowpipe_spend_daily as (
    select
        dates.date,
        'Snowpipe' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        sum(metering_history.credits_used * daily_rates.effective_rate) as spend
    from dates
    left join snowflake.account_usage.metering_history on
        dates.date = convert_timezone('UTC', metering_history.start_time)::date
    left join {{ ref('daily_rates') }} on
        dates.date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    where metering_history.service_type = 'PIPE'
    group by 1, 2, 3, 4
),

query_acceleration_spend_daily as (
    select
        dates.date,
        'Query Acceleration' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        sum(metering_history.credits_used * daily_rates.effective_rate) as spend
    from dates
    left join snowflake.account_usage.metering_history on
        dates.date = convert_timezone('UTC', metering_history.start_time)::date
    left join {{ ref('daily_rates') }} on
        dates.date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    where metering_history.service_type = 'QUERY_ACCELERATION'
    group by 1, 2, 3, 4
),

replication_spend_daily as (
    select
        dates.date,
        'Replication' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        sum(metering_history.credits_used * daily_rates.effective_rate) as spend
    from dates
    left join snowflake.account_usage.metering_history on
        dates.date = convert_timezone('UTC', metering_history.start_time)::date
    left join {{ ref('daily_rates') }} on
        dates.date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    where metering_history.service_type = 'REPLICATION'
    group by 1, 2, 3, 4
),

search_optimization_spend_daily as (
    select
        dates.date,
        'Search Optimization' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        sum(metering_history.credits_used * daily_rates.effective_rate) as spend
    from dates
    left join snowflake.account_usage.metering_history on
        dates.date = convert_timezone('UTC', metering_history.start_time)::date
    left join {{ ref('daily_rates') }} on
        dates.date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    where metering_history.service_type = 'SEARCH_OPTIMIZATION'
    group by 1, 2, 3, 4
)

select * from storage_spend_daily
union all
select * from compute_spend_daily
union all
select * from adj_for_incl_cloud_services_daily
union all
select * from cloud_services_spend_daily
union all
select * from automatic_clustering_spend_daily
union all
select * from materialized_view_spend_daily
union all
select * from snowpipe_spend_daily
union all
select * from query_acceleration_spend_daily
union all
select * from replication_spend_daily
union all
select * from search_optimization_spend_daily
