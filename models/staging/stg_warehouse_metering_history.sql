{{ config(
    materialized='incremental',
    unique_key=['start_time', 'account_locator', 'warehouse_id'],
) }}

select
    {% if var('uses_org_view', false) %}
    organization_name,
    account_name,
    account_locator,
    {% endif %}
    start_time,
    end_time,
    warehouse_id,
    warehouse_name,
    credits_used,
    credits_used_compute,
    credits_used_cloud_services
from {{ source('snowflake_account_usage', 'warehouse_metering_history') }}

{% if is_incremental() %}
    -- account for changing metering data
    where end_time > (select coalesce(dateadd(day, -7, max(end_time)), '1970-01-01') from {{ this }})
{% endif %}

order by start_time
