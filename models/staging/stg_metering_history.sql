{{ config(
    materialized='incremental', 
    unique_key=['service_type', 'start_time', 'entity_id'],
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

{% if is_incremental() %}
    -- account for changing metering data
    where end_time > (select coalesce(dateadd(day, -7, max(end_time)), '1970-01-01') from {{ this }})
{% endif %}

order by start_time asc
