
{{ config(
    materialized='incremental',
    unique_key=['_unique_key', 'query_start_time'],
) }}

with
access_history as (
    select *
    from {{ ref('stg_access_history') }}

    {% if is_incremental() %}
        where query_start_time > (select coalesce(dateadd('day', -1, max(query_start_time)), '1970-01-01') from {{ this }})
    {% endif %}

),

access_history_flattened as (
    select distinct
        -- query can reference same object multiple times
        access_history.query_id,
        access_history.query_start_time,
        access_history.user_name as user_name,
        split_part(objects_accessed.value:objectName::text, '.', 1) as database_name,
        split_part(objects_accessed.value:objectName::text, '.', 2) as schema_name,
        split_part(objects_accessed.value:objectName::text, '.', 3) as table_name,
        objects_accessed.value:columns as columns_array

    from access_history, lateral flatten(access_history.direct_objects_accessed) as objects_accessed
)

select
    *,
    database_name || '.' || schema_name || '.' || table_name as full_table_name,
    md5(concat(query_id, full_table_name)) as _unique_key
from access_history_flattened
where
    database_name is not null
    and schema_name is not null
    and table_name is not null
order by query_start_time asc
