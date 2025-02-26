{{ config(materialized='view') }}

select
    {% if var('uses_org_view', false) %}
    organization_name,
    account_name,
    account_locator,
    {% endif %}
    usage_date as date,
    service_type,
    credits_used_cloud_services,
    credits_adjustment_cloud_services
from {{ source('snowflake_account_usage', 'metering_daily_history') }}
