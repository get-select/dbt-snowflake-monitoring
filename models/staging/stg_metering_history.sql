{{ config(materialized='incremental') }}

select
    name,
    credits_used_compute,
    start_time,
    end_time,
    service_type,
    credits_used_cloud_services,
    credits_used
from {{ source('snowflake_account_usage', 'metering_history') }}

{% if is_incremental() %}
    where end_time > (select max(end_time) from {{ this }})
{% endif %}

order by start_time asc
