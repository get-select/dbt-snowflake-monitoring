{{ config(
    enabled=var('uses_org_view', false),
    alias='daily_spend'
) }}
select
    convert_timezone('UTC', hour)::date as date, -- get UTC date to align with Snowflake billing
    organization_name,
    account_name,
    account_locator,
    service,
    storage_type,
    warehouse_name,
    database_name,
    sum(spend) as spend,
    sum(spend_net_cloud_services) as spend_net_cloud_services,
    any_value(currency) as currency
from {{ ref('hourly_spend_org_level') }}
group by 1, 2, 3, 4, 5, 6, 7, 8
