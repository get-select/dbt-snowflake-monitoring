{% if var('org_usage_view', false) %}
-- Organizational view mode
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
{% else %}
-- Single account mode
select
    convert_timezone('UTC', hour)::date as date, -- get UTC date to align with Snowflake billing
    service,
    storage_type,
    warehouse_name,
    database_name,
    sum(spend) as spend,
    sum(spend_net_cloud_services) as spend_net_cloud_services,
    any_value(currency) as currency
from {{ ref('hourly_spend') }}
group by 1, 2, 3, 4, 5
{% endif %}
