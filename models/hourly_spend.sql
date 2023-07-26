-- depends_on: {{ ref('stg_metering_history') }}
{{ config(materialized='table') }}

with hour_spine as (
    {% if execute %}
{% set stg_metering_history_relation = load_relation(ref('stg_metering_history')) %}
        {% if stg_metering_history_relation %}
            {% set results = run_query("select min(convert_timezone('UTC', start_time)) from " ~ ref('stg_metering_history')) %}
            {% set start_date = "'" ~ results.columns[0][0] ~ "'" %}
            {% set results = run_query("select dateadd(hour, 1, max(convert_timezone('UTC', start_time))) from " ~ ref('stg_metering_history')) %}
            {% set end_date = "'" ~ results.columns[0][0] ~ "'" %}
        {% else %}
            {% set start_date = "'2023-01-01 00:00:00'" %} {# this is just a dummy date for initial compilations before stg_metering_history exists #}
            {% set end_date = "'2023-01-01 01:00:00'" %} {# this is just a dummy date for initial compilations before stg_metering_history exists #}
        {% endif %}
    {% endif %}
{{ dbt_utils.date_spine(
            datepart="hour",
            start_date=start_date,
            end_date=end_date
        )
    }}
),

hours as (
    select
        date_hour as hour,
        hour::date as date,
        count(hour) over (partition by date) as hours_thus_far,
        day(last_day(date)) as days_in_month
    from hour_spine
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

storage_spend_hourly as (
    select
        hours.hour,
        'Storage' as service,
        storage_terabytes_daily.storage_type,
        null as warehouse_name,
        storage_terabytes_daily.database_name,
        coalesce(
            sum(
                div0(
                    storage_terabytes_daily.storage_terabytes,
                    hours.days_in_month * 24
                ) * daily_rates.effective_rate
            ),
            0
        ) as spend,
        spend as spend_net_cloud_services,
        any_value(daily_rates.currency) as currency
    from hours
    left join storage_terabytes_daily on hours.date = convert_timezone('UTC', storage_terabytes_daily.date)
    left join {{ ref('daily_rates') }}
        on storage_terabytes_daily.date = daily_rates.date
            and daily_rates.service_type = 'STORAGE'
            and daily_rates.usage_type = 'storage'
    group by 1, 2, 3, 4, 5
),

data_transfer_spend_hourly as (
    -- Right now we don't have a way of getting this at an hourly grain
    -- We can get source cloud + region, target cloud + region, and bytes transferred at an hourly grain from DATA_TRANSFER_HISTORY
    -- But Snowflake doesn't provide data transfer rates programmatically, so we can't get the cost
    -- We could make a LUT from https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf but it would be a lot of work to maintain and would frequently become out of date
    -- So for now we just use the daily reported usage and evenly distribute it across the day
    select
        hours.hour,
        'Data Transfer' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        coalesce(stg_usage_in_currency_daily.usage_in_currency / hours.hours_thus_far, 0) as spend,
        spend as spend_net_cloud_services,
        stg_usage_in_currency_daily.currency as currency
    from hours
    left join {{ ref('stg_usage_in_currency_daily') }} on
        stg_usage_in_currency_daily.account_locator = {{ account_locator() }}
        and stg_usage_in_currency_daily.usage_type = 'data transfer'
        and hours.hour::date = stg_usage_in_currency_daily.usage_date
),

compute_spend_hourly as (
    select
        hours.hour,
        'Compute' as service,
        null as storage_type,
        stg_metering_history.name as warehouse_name,
        null as database_name,
        coalesce(
            sum(
                stg_metering_history.credits_used_compute * daily_rates.effective_rate
            ),
            0
        ) as spend,
        spend as spend_net_cloud_services,
        any_value(daily_rates.currency) as currency
    from hours
    left join {{ ref('stg_metering_history') }} on
        hours.hour = convert_timezone(
            'UTC', stg_metering_history.start_time
        )
    left join {{ ref('daily_rates') }}
        on hours.hour::date = daily_rates.date
            and daily_rates.service_type = 'COMPUTE'
            and daily_rates.usage_type = 'compute'
    where
        stg_metering_history.service_type = 'WAREHOUSE_METERING' and stg_metering_history.name != 'CLOUD_SERVICES_ONLY'
    group by 1, 2, 3, 4
),

serverless_task_spend_hourly as (
    select
        hours.hour,
        'Serverless Tasks' as service,
        null as storage_type,
        null as warehouse_name,
        stg_serverless_task_history.database_name,
        coalesce(
            sum(
                stg_serverless_task_history.credits_used * daily_rates.effective_rate
            ),
            0
        ) as spend,
        spend as spend_net_cloud_services,
        any_value(daily_rates.currency) as currency
    from hours
    left join {{ ref('stg_serverless_task_history') }} on
        hours.hour = date_trunc('hour', stg_serverless_task_history.start_time)
    left join {{ ref('daily_rates') }}
        on hours.hour::date = daily_rates.date
            and daily_rates.service_type = 'COMPUTE'
            and daily_rates.usage_type = 'serverless tasks'
    group by 1, 2, 3, 4, 5
),

adj_for_incl_cloud_services_hourly as (
    select
        hours.hour,
        'Adj For Incl Cloud Services' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        coalesce(
            sum(
                stg_metering_daily_history.credits_adjustment_cloud_services * daily_rates.effective_rate
            ),
            0
        ) as spend,
        0 as spend_net_cloud_services,
        any_value(daily_rates.currency) as currency
    from hours
    left join {{ ref('stg_metering_daily_history') }} on
        hours.hour = stg_metering_daily_history.date
    left join {{ ref('daily_rates') }}
        on hours.hour::date = daily_rates.date
            and daily_rates.service_type = 'COMPUTE'
            and daily_rates.usage_type = 'cloud services'
    group by 1, 2, 3, 4
),

_cloud_services_usage_hourly as (
    select
        hours.hour,
        hours.date,
        'Cloud Services' as service,
        null as storage_type,
        case
            when
                stg_metering_history.name = 'CLOUD_SERVICES_ONLY' then 'Cloud Services Only'
            else stg_metering_history.name
        end as warehouse_name,
        null as database_name,
        coalesce(
            sum(stg_metering_history.credits_used_cloud_services), 0
        ) as credits_used_cloud_services
    from hours
    left join {{ ref('stg_metering_history') }} on
        hours.hour = convert_timezone(
            'UTC', stg_metering_history.start_time
        )
        and stg_metering_history.service_type = 'WAREHOUSE_METERING'
    group by 1, 2, 3, 4, 5
),

_cloud_services_billed_daily as (
    select
        date,
        sum(credits_used_cloud_services) as credits_used_cloud_services,
        sum(
            credits_used_cloud_services + credits_adjustment_cloud_services
        ) as credits_used_cloud_services_billable
    from {{ ref('stg_metering_daily_history') }}
    where
        service_type = 'WAREHOUSE_METERING'
    group by 1
),

cloud_services_spend_hourly as (
    select
        _cloud_services_usage_hourly.hour,
        _cloud_services_usage_hourly.service,
        _cloud_services_usage_hourly.storage_type,
        _cloud_services_usage_hourly.warehouse_name,
        _cloud_services_usage_hourly.database_name,
        _cloud_services_usage_hourly.credits_used_cloud_services * daily_rates.effective_rate as spend,

        (
            div0(
                _cloud_services_usage_hourly.credits_used_cloud_services,
                _cloud_services_billed_daily.credits_used_cloud_services
            ) * _cloud_services_billed_daily.credits_used_cloud_services_billable
        ) * daily_rates.effective_rate as spend_net_cloud_services,
        daily_rates.currency
    from _cloud_services_usage_hourly
    inner join _cloud_services_billed_daily on
        _cloud_services_usage_hourly.date = _cloud_services_billed_daily.date
    left join {{ ref('daily_rates') }}
        on _cloud_services_usage_hourly.date = daily_rates.date
            and daily_rates.service_type = 'COMPUTE'
            and daily_rates.usage_type = 'cloud services'

),

automatic_clustering_spend_hourly as (
    select
        hours.hour,
        'Automatic Clustering' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        coalesce(
            sum(
                stg_metering_history.credits_used * daily_rates.effective_rate
            ),
            0
        ) as spend,
        spend as spend_net_cloud_services,
        any_value(daily_rates.currency) as currency
    from hours
    left join {{ ref('stg_metering_history') }} on
        hours.hour = convert_timezone(
            'UTC', stg_metering_history.start_time
        )
        and stg_metering_history.service_type = 'AUTO_CLUSTERING'
    left join {{ ref('daily_rates') }}
        on hours.hour::date = daily_rates.date
            and daily_rates.service_type = 'COMPUTE'
            and daily_rates.usage_type = 'automatic clustering'
    group by 1, 2, 3, 4
),

materialized_view_spend_hourly as (
    select
        hours.hour,
        'Materialized Views' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        coalesce(
            sum(
                stg_metering_history.credits_used * daily_rates.effective_rate
            ),
            0
        ) as spend,
        spend as spend_net_cloud_services,
        any_value(daily_rates.currency) as currency
    from hours
    left join {{ ref('stg_metering_history') }} on
        hours.hour = convert_timezone(
            'UTC', stg_metering_history.start_time
        )
        and stg_metering_history.service_type = 'MATERIALIZED_VIEW'
    left join {{ ref('daily_rates') }}
        on hours.hour::date = daily_rates.date
            and daily_rates.service_type = 'COMPUTE'
            and daily_rates.usage_type = 'materialized views'
    group by 1, 2, 3, 4
),

snowpipe_spend_hourly as (
    select
        hours.hour,
        'Snowpipe' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        coalesce(
            sum(
                stg_metering_history.credits_used * daily_rates.effective_rate
            ),
            0
        ) as spend,
        spend as spend_net_cloud_services,
        any_value(daily_rates.currency) as currency
    from hours
    left join {{ ref('stg_metering_history') }} on
        hours.hour = convert_timezone(
            'UTC', stg_metering_history.start_time
        )
        and stg_metering_history.service_type = 'PIPE'
    left join {{ ref('daily_rates') }}
        on hours.hour::date = daily_rates.date
            and daily_rates.service_type = 'COMPUTE'
            and daily_rates.usage_type = 'snowpipe'
    group by 1, 2, 3, 4
),

query_acceleration_spend_hourly as (
    select
        hours.hour,
        'Query Acceleration' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        coalesce(
            sum(
                stg_metering_history.credits_used * daily_rates.effective_rate
            ),
            0
        ) as spend,
        spend as spend_net_cloud_services,
        any_value(daily_rates.currency) as currency
    from hours
    left join {{ ref('stg_metering_history') }} on
        hours.hour = convert_timezone(
            'UTC', stg_metering_history.start_time
        )
        and stg_metering_history.service_type = 'QUERY_ACCELERATION'
    left join {{ ref('daily_rates') }}
        on hours.hour::date = daily_rates.date
            and daily_rates.service_type = 'COMPUTE'
            and daily_rates.usage_type = 'query acceleration'
    group by 1, 2, 3, 4
),

replication_spend_hourly as (
    select
        hours.hour,
        'Replication' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        coalesce(
            sum(
                stg_metering_history.credits_used * daily_rates.effective_rate
            ),
            0
        ) as spend,
        spend as spend_net_cloud_services,
        any_value(daily_rates.currency) as currency
    from hours
    left join {{ ref('stg_metering_history') }} on
        hours.hour = convert_timezone(
            'UTC', stg_metering_history.start_time
        )
        and stg_metering_history.service_type = 'REPLICATION'
    left join {{ ref('daily_rates') }}
        on hours.hour::date = daily_rates.date
            and daily_rates.service_type = 'COMPUTE'
            and daily_rates.usage_type = 'replication'
    group by 1, 2, 3, 4
),

search_optimization_spend_hourly as (
    select
        hours.hour,
        'Search Optimization' as service,
        null as storage_type,
        null as warehouse_name,
        null as database_name,
        coalesce(
            sum(
                stg_metering_history.credits_used * daily_rates.effective_rate
            ),
            0
        ) as spend,
        spend as spend_net_cloud_services,
        any_value(daily_rates.currency) as currency
    from hours
    left join {{ ref('stg_metering_history') }} on
        hours.hour = convert_timezone(
            'UTC', stg_metering_history.start_time
        )
        and stg_metering_history.service_type = 'SEARCH_OPTIMIZATION'
    left join {{ ref('daily_rates') }}
        on hours.hour::date = daily_rates.date
            and daily_rates.service_type = 'COMPUTE'
            and daily_rates.usage_type = 'search optimization'
    group by 1, 2, 3, 4
),

unioned as (
    select * from storage_spend_hourly
    union all
    select * from data_transfer_spend_hourly
    union all
    select * from compute_spend_hourly
    union all
    select * from adj_for_incl_cloud_services_hourly
    union all
    select * from cloud_services_spend_hourly
    union all
    select * from automatic_clustering_spend_hourly
    union all
    select * from materialized_view_spend_hourly
    union all
    select * from snowpipe_spend_hourly
    union all
    select * from query_acceleration_spend_hourly
    union all
    select * from replication_spend_hourly
    union all
    select * from search_optimization_spend_hourly
    union all
    select * from serverless_task_spend_hourly
)

select
    convert_timezone('UTC', hour)::timestamp_ltz as hour,
    service,
    storage_type,
    warehouse_name,
    database_name,
    spend,
    spend_net_cloud_services,
    currency
from unioned
