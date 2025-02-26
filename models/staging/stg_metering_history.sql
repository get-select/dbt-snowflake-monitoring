{{ config(
    materialized='table'
) }}

select
    {% if var('uses_org_view', false) %}
    organization_name,
    account_name,
    account_locator,
    {% endif %}
    service_type,
    start_time,
    end_time,
    entity_id,
    name,
    credits_used_compute,
    credits_used_cloud_services,
    credits_used
from {{ source('snowflake_account_usage', 'metering_history') }}
order by start_time asc
