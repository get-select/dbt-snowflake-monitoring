{{ config(materialized='incremental') }}

select
    {{ add_account_columns() }}
    query_id,
    parent_query_id,
    root_query_id,
    query_start_time,
    user_name,
    direct_objects_accessed,
    base_objects_accessed,
    objects_modified
from {{ source('snowflake_account_usage', 'access_history') }}

{% if is_incremental() %}
    where query_start_time > (select coalesce(max(query_start_time), '1970-01-01') from {{ this }})
{% endif %}

order by query_start_time asc
