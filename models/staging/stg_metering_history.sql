{{ config(
    materialized='table',
    enabled=not(var('uses_org_view', false))
) }}
-- This model is temporary disabled for Organisation Account views until Snowflake includes metering_history
-- in the Organisation Account. They are planning this for Q2 (May - Jul) 2025.
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
