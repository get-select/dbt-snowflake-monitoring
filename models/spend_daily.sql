{{ config(materialized='table') }}

with dates as (
{{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2019-01-01' as date)",
        end_date="cast('2030-01-01' as date)"
    )
    }}
),

dates_with_days_in_month as (
    select
        date_day as date,
        day(last_day(date_day)) as days_in_month
    from dates
),

storage_terabytes_daily as (
    select
        usage_date,
        'storage' as usage_type,
        'table and time travel' as usage_type_granular,
        storage_bytes / power(1024, 4) as storage_terabytes
    from snowflake.account_usage.storage_usage
    union all
    select
        usage_date,
        'storage' as usage_type,
        'stage' as usage_type_granular,
        stage_bytes / power(1024, 4) as storage_terabytes
    from snowflake.account_usage.storage_usage
    union all
    select
        usage_date,
        'storage' as usage_type,
        'failsafe' as usage_type_granular,
        failsafe_bytes / power(1024, 4) as storage_terabyte
    from snowflake.account_usage.storage_usage
)

, storage_spend_daily as (
    select
        storage_terabytes_daily.usage_date,
        storage_terabytes_daily.usage_type,
        storage_terabytes_daily.usage_type_granular,
        coalesce(storage_terabytes_daily.storage_terabytes * daily_rates.effective_rate, 0) as spend_usd
    from storage_terabytes_daily
    left join dates_with_days_in_month on storage_terabytes_daily.usage_date = dates_with_days_in_month.date
    left join {{ ref('daily_rates') }} on
        storage_terabytes_daily.usage_date = daily_rates.date
        and daily_rates.usage_type = 'storage'
),

compute_spend_daily as (
    select
        metering_history.start_time::date as usage_date,
        'warehouse compute' as usage_type,
        metering_history.name as usage_type_granular,
        sum(metering_history.credits_used_compute * daily_rates.effective_rate) as spend_usd
    from snowflake.account_usage.metering_history
    left join {{ ref('daily_rates') }} on
        metering_history.start_time::date = daily_rates.date
        and daily_rates.usage_type = 'compute'
        and daily_rates.service_type = 'COMPUTE'
    where metering_history.service_type = 'WAREHOUSE_METERING'
    group by 1, 2, 3
),

cloud_services_spend_daily as (
    select
        metering_history.start_time::date as usage_date,
        'cloud services' as usage_type,
        metering_history.name as usage_type_granular,
        sum(metering_history.credits_used_cloud_services * daily_rates.effective_rate) as spend_usd
    from snowflake.account_usage.metering_history
    left join {{ ref('daily_rates') }} on
        metering_history.start_time::date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    where metering_history.service_type = 'WAREHOUSE_METERING'
    group by 1, 2, 3
),

automatic_clustering_spend_daily as (
    select
        metering_history.start_time::date as usage_date,
        'serverless' as usage_type,
        'automatic clustering' as usage_type_granular,
        sum(metering_history.credits_used * daily_rates.effective_rate) as spend_usd
    from snowflake.account_usage.metering_history
    left join {{ ref('daily_rates') }} on
        metering_history.start_time::date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    where metering_history.service_type = 'AUTO_CLUSTERING'
    group by 1, 2, 3
),

materialized_view_spend_daily as (
    select
        metering_history.start_time::date as usage_date,
        'serverless' as usage_type,
        'materialized view' as usage_type_granular,
        sum(metering_history.credits_used * daily_rates.effective_rate) as spend_usd
    from snowflake.account_usage.metering_history
    left join {{ ref('daily_rates') }} on
        metering_history.start_time::date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    where metering_history.service_type = 'MATERIALIZED_VIEW'
    group by 1, 2, 3
),

snowpipe_spend_daily as (
    select
        metering_history.start_time::date as usage_date,
        'serverless' as usage_type,
        'snowpipe' as usage_type_granular,
        sum(metering_history.credits_used * daily_rates.effective_rate) as spend_usd
    from snowflake.account_usage.metering_history
    left join {{ ref('daily_rates') }} on
        metering_history.start_time::date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    where metering_history.service_type = 'PIPE'
    group by 1, 2, 3
),

query_acceleration_spend_daily as (
    select
        metering_history.start_time::date as usage_date,
        'serverless' as usage_type,
        'query acceleration' as usage_type_granular,
        sum(metering_history.credits_used * daily_rates.effective_rate) as spend_usd
    from snowflake.account_usage.metering_history
    left join {{ ref('daily_rates') }} on
        metering_history.start_time::date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    where metering_history.service_type = 'QUERY_ACCELERATION'
    group by 1, 2, 3
),

replication_spend_daily as (
    select
        metering_history.start_time::date as usage_date,
        'serverless' as usage_type,
        'replication' as usage_type_granular,
        sum(metering_history.credits_used * daily_rates.effective_rate) as spend_usd
    from snowflake.account_usage.metering_history
    left join {{ ref('daily_rates') }} on
        metering_history.start_time::date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    where metering_history.service_type = 'REPLICATION'
    group by 1, 2, 3
),

search_optimization_spend_daily as (
    select
        metering_history.start_time::date as usage_date,
        'serverless' as usage_type,
        'replication' as usage_type_granular,
        sum(metering_history.credits_used * daily_rates.effective_rate) as spend_usd
    from snowflake.account_usage.metering_history
    left join {{ ref('daily_rates') }} on
        metering_history.start_time::date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    where metering_history.service_type = 'SEARCH_OPTIMIZATION'
    group by 1, 2, 3
)

select * from storage_spend_daily
union all
select * from compute_spend_daily
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
union all
select * from storage_spend_daily
