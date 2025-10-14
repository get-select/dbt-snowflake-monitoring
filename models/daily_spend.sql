select
    convert_timezone('UTC', hour)::date as date, -- get UTC date to align with Snowflake billing
    service,
    storage_type,
    warehouse_name,
    database_name,
    sum(usage) as usage,
    sum(usage_net_cloud_services) as usage_net_cloud_services,
    sum(spend) as spend,
    sum(spend_net_cloud_services) as spend_net_cloud_services,
    any_value(currency) as currency,
    any_value(usage_unit) as usage_unit,
    -- Weighted average rates across the day. Typically rates are consistent
    -- throughout a day so this should still be fairly accurate.
    case
        when sum(usage) > 0 then sum(spend) / sum(usage)
        when sum(usage_net_cloud_services) > 0 then sum(spend_net_cloud_services) / sum(usage_net_cloud_services)
        else 0
    end as usage_rate
from {{ ref('hourly_spend') }}
group by 1, 2, 3, 4, 5
