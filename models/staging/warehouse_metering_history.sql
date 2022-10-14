{{ config(materialized='incremental') }}

select
    start_time,
    end_time,
    warehouse_id,
    warehouse_name,
    credits_used,
    credits_used_compute,
    credits_used_cloud_services
from {{ source('snowflake_account_usage', 'warehouse_metering_history')}}

{% if is_incremental() %}
  where end_time > (select max(end_time) from {{ this }})
{% endif %}

order by start_time