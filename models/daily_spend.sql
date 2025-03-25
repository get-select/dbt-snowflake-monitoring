{{
config(
    enabled=not(var('uses_org_view', false)))
}}
-- This model is temporary disabled for Organisation Account views until Snowflake includes metering_history
-- and severless_task_history views in the Organisation Account. They are planning this for Q2 (May - Jul) 2025.
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
