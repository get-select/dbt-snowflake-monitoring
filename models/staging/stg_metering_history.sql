{{ config(
    materialized='table',
    enabled=not(var('uses_org_view', false))
) }}

select
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
