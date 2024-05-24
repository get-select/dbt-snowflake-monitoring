{{ config(materialized='incremental') }}

select
    timestamp,
    warehouse_id,
    warehouse_name,
    cluster_number,
    event_name,
    event_reason,
    event_state,
    user_name,
    role_name,
    query_id
from {{ source('snowflake_account_usage', 'warehouse_events_history') }}

{% if is_incremental() %}
    where timestamp > (select coalesce(max(timestamp), '1970-01-01') from {{ this }})
{% endif %}

order by timestamp asc
