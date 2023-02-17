-- depends_on: {{ ref('stg_warehouse_metering_history') }}
{{ config(materialized='table') }}

with date_spine as (
    {% if execute %}
{% set stg_warehouse_metering_history_relation = load_relation(ref('stg_warehouse_metering_history')) %}
        {% if stg_warehouse_metering_history_relation %}
            {% set results = run_query("select dateadd(day, 1, timestampadd(hour, -1, convert_timezone('UTC', min(start_time)))::date) from " ~ ref('stg_warehouse_metering_history')) %} {# first complete day #} -- noqa
{% set start_date = "'" ~ results.columns[0][0] ~ "'" %}
        {% else %}
            {% set start_date = "dateadd(day, -1, convert_timezone('UTC', current_timestamp)::date)" %} {# this is just a dummy date for initial compilations before stg_warehouse_metering_history exists #}
        {% endif %}
    {% endif %}
{{ dbt_utils.date_spine(
            datepart="day",
            start_date=start_date,
            end_date="convert_timezone('UTC', current_timestamp)::date"
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
),

storage_spend_daily as (
    select
        storage_terabytes_daily.date,
        'Storage' as service,
        storage_terabytes_daily.storage_type,
        null as warehouse_name,
        storage_terabytes_daily.database_name,
        coalesce(sum(div0(storage_terabytes_daily.storage_terabytes, dates.days_in_month) * daily_rates.effective_rate), 0) as spend,
        spend as spend_net_cloud_services
    from dates
    left join storage_terabytes_daily on dates.date = storage_terabytes_daily.date
    inner join {{ ref('daily_rates') }}
        on storage_terabytes_daily.date = daily_rates.date
            and daily_rates.service_type = 'STORAGE'
            and daily_rates.usage_type = 'storage'
    group by 1, 2, 3, 4, 5
),

compute_spend_daily as (
    select
        dates.date,
        'Compute' as service,
        null as storage_type,
        stg_metering_history.name as warehouse_name,
        null as database_name,
        coalesce(sum(stg_metering_history.credits_used_compute * daily_rates.effective_rate), 0) as spend,
        spend as spend_net_cloud_services
    from dates
    left join {{ ref('stg_metering_history') }} on
        dates.date = convert_timezone('UTC', stg_metering_history.start_time)::date
    inner join {{ ref('daily_rates') }}
        on dates.date = daily_rates.date
            and daily_rates.service_type = 'COMPUTE'
            and daily_rates.usage_type = 'compute'
    where stg_metering_history.service_type = 'WAREHOUSE_METERING' and stg_metering_history.name != 'CLOUD_SERVICES_ONLY'
    group by 1, 2, 3, 4
),

serverless_task_spend_daily as (
    select
        dates.date,
        'Serverless Tasks' as service,
        null as storage_type,
        null as warehouse_name,
        stg_serverless_task_history.database_name,
        coalesce(sum(stg_serverless_task_history.credits_used * daily_rates.effective_rate), 0) as spend,
        spend as spend_net_cloud_services
    from dates
    left join {{ ref('stg_serverless_task_history') }} on
        dates.date = convert_timezone('UTC', stg_serverless_task_history.start_time)::date
    inner join {{ ref('daily_rates') }}
        on dates.date = daily_rates.date
            and daily_rates.service_type = 'COMPUTE'
            and daily_rates.usage_type = 'serverless tasks'
    group by 1, 2, 3, 4, 5
),

adj_for_incl_cloud_services_daily as (
    select
        dates.date,
        'Adj For Incl Cloud Services' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        coalesce(sum(stg_metering_daily_history.credits_adjustment_cloud_services * daily_rates.effective_rate), 0) as spend,
        0 as spend_net_cloud_services
    from dates
    left join {{ ref('stg_metering_daily_history') }} on
        dates.date = stg_metering_daily_history.date
    inner join {{ ref('daily_rates') }}
        on dates.date = daily_rates.date
            and daily_rates.service_type = 'COMPUTE'
            and daily_rates.usage_type = 'cloud services'
    group by 1, 2, 3, 4
),

_cloud_services_spend_daily as (
    select
        dates.date,
        'Cloud Services' as service,
        null as storage_type,
        case when stg_metering_history.name = 'CLOUD_SERVICES_ONLY' then 'Cloud Services Only' else stg_metering_history.name end as warehouse_name,
        null as database_name,
        coalesce(sum(stg_metering_history.credits_used_cloud_services), 0) as credits_used_cloud_services,
        any_value(daily_rates.effective_rate) as effective_rate
    from dates
    left join {{ ref('stg_metering_history') }} on
        dates.date = convert_timezone('UTC', stg_metering_history.start_time)::date
        and stg_metering_history.service_type = 'WAREHOUSE_METERING'
    inner join {{ ref('daily_rates') }}
        on dates.date = daily_rates.date
            and daily_rates.service_type = 'COMPUTE'
            and daily_rates.usage_type = 'cloud services'
    group by 1, 2, 3, 4
),


credits_billed_daily as (
    select
        convert_timezone('UTC', start_time)::date as date,
        sum(credits_used_compute) as daily_credits_used_compute,
        sum(credits_used_cloud_services) as daily_credits_used_cloud_services,
        greatest(daily_credits_used_cloud_services - daily_credits_used_compute * 0.1, 0) as daily_billable_cloud_services
    from {{ ref('stg_metering_history') }}
    group by 1
),

cloud_services_spend_daily as (
    select
        _cloud_services_spend_daily.date,
        _cloud_services_spend_daily.service,
        _cloud_services_spend_daily.storage_type,
        _cloud_services_spend_daily.warehouse_name,
        _cloud_services_spend_daily.database_name,
        _cloud_services_spend_daily.credits_used_cloud_services * _cloud_services_spend_daily.effective_rate as spend,

        (div0(_cloud_services_spend_daily.credits_used_cloud_services, credits_billed_daily.daily_credits_used_cloud_services) * credits_billed_daily.daily_billable_cloud_services) * _cloud_services_spend_daily.effective_rate as spend_net_cloud_services
    from _cloud_services_spend_daily
    inner join credits_billed_daily on
         _cloud_services_spend_daily.date = credits_billed_daily.date

),

automatic_clustering_spend_daily as (
    select
        dates.date,
        'Automatic Clustering' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        coalesce(sum(stg_metering_history.credits_used * daily_rates.effective_rate), 0) as spend,
        spend as spend_net_cloud_services
    from dates
    left join {{ ref('stg_metering_history') }} on
        dates.date = convert_timezone('UTC', stg_metering_history.start_time)::date
        and stg_metering_history.service_type = 'AUTO_CLUSTERING'
    inner join {{ ref('daily_rates') }}
        on dates.date = daily_rates.date
            and daily_rates.service_type = 'COMPUTE'
            and daily_rates.usage_type = 'automatic clustering'
    group by 1, 2, 3, 4
),

materialized_view_spend_daily as (
    select
        dates.date,
        'Materialized Views' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        coalesce(sum(stg_metering_history.credits_used * daily_rates.effective_rate), 0) as spend,
        spend as spend_net_cloud_services
    from dates
    left join {{ ref('stg_metering_history') }} on
        dates.date = convert_timezone('UTC', stg_metering_history.start_time)::date
        and stg_metering_history.service_type = 'MATERIALIZED_VIEW'
    inner join {{ ref('daily_rates') }}
        on dates.date = daily_rates.date
            and daily_rates.service_type = 'COMPUTE'
            and daily_rates.usage_type = 'materialized view' {# TODO: need someone to confirm whether its materialized 'view' or 'views' #}
    group by 1, 2, 3, 4
),

snowpipe_spend_daily as (
    select
        dates.date,
        'Snowpipe' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        coalesce(sum(stg_metering_history.credits_used * daily_rates.effective_rate), 0) as spend,
        spend as spend_net_cloud_services
    from dates
    left join {{ ref('stg_metering_history') }} on
        dates.date = convert_timezone('UTC', stg_metering_history.start_time)::date
        and stg_metering_history.service_type = 'PIPE'
    inner join {{ ref('daily_rates') }}
        on dates.date = daily_rates.date
            and daily_rates.service_type = 'COMPUTE'
            and daily_rates.usage_type = 'snowpipe'
    group by 1, 2, 3, 4
),

query_acceleration_spend_daily as (
    select
        dates.date,
        'Query Acceleration' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        coalesce(sum(stg_metering_history.credits_used * daily_rates.effective_rate), 0) as spend,
        spend as spend_net_cloud_services
    from dates
    left join {{ ref('stg_metering_history') }} on
        dates.date = convert_timezone('UTC', stg_metering_history.start_time)::date
        and stg_metering_history.service_type = 'QUERY_ACCELERATION'
    inner join {{ ref('daily_rates') }}
        on dates.date = daily_rates.date
            and daily_rates.service_type = 'COMPUTE'
            and daily_rates.usage_type = 'query acceleration'
    group by 1, 2, 3, 4
),

replication_spend_daily as (
    select
        dates.date,
        'Replication' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        coalesce(sum(stg_metering_history.credits_used * daily_rates.effective_rate), 0) as spend,
        spend as spend_net_cloud_services
    from dates
    left join {{ ref('stg_metering_history') }} on
        dates.date = convert_timezone('UTC', stg_metering_history.start_time)::date
        and stg_metering_history.service_type = 'REPLICATION'
    inner join {{ ref('daily_rates') }}
        on dates.date = daily_rates.date
            and daily_rates.service_type = 'COMPUTE'
            and daily_rates.usage_type = 'replication'
    group by 1, 2, 3, 4
),

search_optimization_spend_daily as (
    select
        dates.date,
        'Search Optimization' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        coalesce(sum(stg_metering_history.credits_used * daily_rates.effective_rate), 0) as spend,
        spend as spend_net_cloud_services
    from dates
    left join {{ ref('stg_metering_history') }} on
        dates.date = convert_timezone('UTC', stg_metering_history.start_time)::date
        and stg_metering_history.service_type = 'SEARCH_OPTIMIZATION'
    inner join {{ ref('daily_rates') }}
        on dates.date = daily_rates.date
            and daily_rates.service_type = 'COMPUTE'
            and daily_rates.usage_type = 'search optimization`'
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
union all
select * from serverless_task_spend_daily
