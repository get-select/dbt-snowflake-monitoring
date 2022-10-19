{{ config(materialized='incremental') }}

with date_spine as (

    {# In a full refresh, this logic sets the start_date of the date spine to the first date in stg_metering_history #}
    {# In incremental mode, this logic sets the start_date of the date spine to be the day after the max current date in this model #}
    {# The end_date is always set to today's date (the date_spine function excludes this date) #}

    {% if is_incremental() %}
    {% set results = run_query("select dateadd(day, 1, max(date)) from " ~ this) %}
    {% if execute %}
    {% set start_date = "'" ~ results.columns[0][0] ~ "'" %}
    {% endif %}
    {% else %}
    {% set results = run_query("select min(convert_timezone('UTC', start_time)::date) from " ~ ref('stg_metering_history')) %}
    {% if execute %}
        {% set start_date = "'" ~ results.columns[0][0] ~ "'" %}
    {% endif %}
{% endif %}

    {% set results = run_query("select " ~ start_date ~ "::date < convert_timezone('UTC', current_timestamp)::date") %}
    {% if execute %}
    {% set should_run = results.columns[0][0] %}
    {% endif %}

    {% if should_run %}
    {{ dbt_utils.date_spine(
            datepart="day",
            start_date=start_date ~ "::date",
            end_date="convert_timezone('UTC', current_timestamp)::date"
        )
    }}
{% else %}
    select '2000-01-01'::date as date_day
    where false
    {% endif %}
),

dates as (
    select
        date_day as date,
        day(last_day(date_day)) as days_in_month
    from date_spine
),

storage_terabytes_daily as (
    select
        date,
        'Table and Time Travel' as storage_type,
        database_name,
        sum(average_database_bytes) / power(1024, 4) as storage_terabytes
    from {{ ref('stg_database_storage_usage_history') }}
    group by 1, 2, 3
    union all
    select
        date,
        'Failsafe' as storage_type,
        database_name,
        sum(average_failsafe_bytes) / power(1024, 4) as storage_terabytes
    from {{ ref('stg_database_storage_usage_history') }}
    group by 1, 2, 3
    union all
    select
        date,
        'Stage' as storage_type,
        null as database_name,
        sum(average_stage_bytes) / power(1024, 4) as storage_terabytes
    from {{ ref('stg_stage_storage_usage_history') }}
    group by 1, 2, 3
)

, storage_spend_daily as (
    select
        storage_terabytes_daily.date,
        'Storage' as service,
        storage_terabytes_daily.storage_type,
        null as warehouse_name,
        storage_terabytes_daily.database_name,
        coalesce(storage_terabytes_daily.storage_terabytes / dates.days_in_month * daily_rates.effective_rate, 0) as spend
    from dates
    left join storage_terabytes_daily on dates.date = storage_terabytes_daily.date
    left join {{ ref('daily_rates') }} on
        storage_terabytes_daily.date = daily_rates.date
        and daily_rates.usage_type = 'storage'
),

compute_spend_daily as (
    select
        dates.date,
        'Compute' as service,
        null as storage_type,
        stg_metering_history.name as warehouse_name,
        null as database_name,
        sum(stg_metering_history.credits_used_compute * daily_rates.effective_rate) as spend
    from dates
    left join {{ ref('stg_metering_history') }} on
        dates.date = convert_timezone('UTC', stg_metering_history.start_time)::date
    left join {{ ref('daily_rates') }} on
        dates.date = daily_rates.date
        and daily_rates.usage_type = 'compute'
        and daily_rates.service_type = 'COMPUTE'
    where stg_metering_history.service_type = 'WAREHOUSE_METERING' and stg_metering_history.name != 'CLOUD_SERVICES_ONLY'
    group by 1, 2, 3, 4
),

adj_for_incl_cloud_services_daily as (
    select
        dates.date,
        'Adj For Incl Cloud Services' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        coalesce(sum(stg_metering_daily_history.credits_adjustment_cloud_services * daily_rates.effective_rate), 0) as spend
    from dates
    left join {{ ref('stg_metering_daily_history') }} on
        dates.date = stg_metering_daily_history.date
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
        case when stg_metering_history.name = 'CLOUD_SERVICES_ONLY' then 'Cloud Services Only' else stg_metering_history.name end as warehouse_name,
        null as database_name,
        sum(stg_metering_history.credits_used_cloud_services * daily_rates.effective_rate) as spend
    from dates
    left join {{ ref('stg_metering_history') }} on
        dates.date = convert_timezone('UTC', stg_metering_history.start_time)::date
    left join {{ ref('daily_rates') }} on
        dates.date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    where stg_metering_history.service_type = 'WAREHOUSE_METERING'
    group by 1, 2, 3, 4
),

automatic_clustering_spend_daily as (
    select
        dates.date,
        'Automatic Clustering' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        sum(stg_metering_history.credits_used * daily_rates.effective_rate) as spend
    from dates
    left join {{ ref('stg_metering_history') }} on
        dates.date = convert_timezone('UTC', stg_metering_history.start_time)::date
    left join {{ ref('daily_rates') }} on
        dates.date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    where stg_metering_history.service_type = 'AUTO_CLUSTERING'
    group by 1, 2, 3, 4
),

materialized_view_spend_daily as (
    select
        dates.date,
        'Materialized Views' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        sum(stg_metering_history.credits_used * daily_rates.effective_rate) as spend
    from dates
    left join {{ ref('stg_metering_history') }} on
        dates.date = convert_timezone('UTC', stg_metering_history.start_time)::date
    left join {{ ref('daily_rates') }} on
        dates.date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    where stg_metering_history.service_type = 'MATERIALIZED_VIEW'
    group by 1, 2, 3, 4
),

snowpipe_spend_daily as (
    select
        dates.date,
        'Snowpipe' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        sum(stg_metering_history.credits_used * daily_rates.effective_rate) as spend
    from dates
    left join {{ ref('stg_metering_history') }} on
        dates.date = convert_timezone('UTC', stg_metering_history.start_time)::date
    left join {{ ref('daily_rates') }} on
        dates.date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    where stg_metering_history.service_type = 'PIPE'
    group by 1, 2, 3, 4
),

query_acceleration_spend_daily as (
    select
        dates.date,
        'Query Acceleration' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        sum(stg_metering_history.credits_used * daily_rates.effective_rate) as spend
    from dates
    left join {{ ref('stg_metering_history') }} on
        dates.date = convert_timezone('UTC', stg_metering_history.start_time)::date
    left join {{ ref('daily_rates') }} on
        dates.date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    where stg_metering_history.service_type = 'QUERY_ACCELERATION'
    group by 1, 2, 3, 4
),

replication_spend_daily as (
    select
        dates.date,
        'Replication' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        sum(stg_metering_history.credits_used * daily_rates.effective_rate) as spend
    from dates
    left join {{ ref('stg_metering_history') }} on
        dates.date = convert_timezone('UTC', stg_metering_history.start_time)::date
    left join {{ ref('daily_rates') }} on
        dates.date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    where stg_metering_history.service_type = 'REPLICATION'
    group by 1, 2, 3, 4
),

search_optimization_spend_daily as (
    select
        dates.date,
        'Search Optimization' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        sum(stg_metering_history.credits_used * daily_rates.effective_rate) as spend
    from dates
    left join {{ ref('stg_metering_history') }} on
        dates.date = convert_timezone('UTC', stg_metering_history.start_time)::date
    left join {{ ref('daily_rates') }} on
        dates.date = daily_rates.date
        and daily_rates.usage_type = 'cloud services'
        and daily_rates.service_type = 'COMPUTE'
    where stg_metering_history.service_type = 'SEARCH_OPTIMIZATION'
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
