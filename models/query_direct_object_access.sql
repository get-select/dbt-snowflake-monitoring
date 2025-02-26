{{ config(
    materialized='incremental',
    unique_key=['_unique_id', 'query_start_time'],
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
    select
        access_history.query_id,
        access_history.query_start_time,
        access_history.user_name,
        {% if var('uses_org_view', false) %}
        access_history.organization_name,
        access_history.account_name,
        access_history.account_locator,
        {% endif %}
        objects_accessed.value:objectId::integer as table_id, -- will be null for secured views or tables from a data share
        objects_accessed.value:objectName::text as object_name,
        objects_accessed.value:objectDomain::text as object_domain,
        objects_accessed.value:columns as columns_array

    from access_history, lateral flatten(access_history.direct_objects_accessed) as objects_accessed
),

access_history_flattened_w_columns as (
    select
        access_history_flattened.query_id,
        access_history_flattened.query_start_time,
        access_history_flattened.user_name,
        {% if var('uses_org_view', false) %}
        access_history_flattened.organization_name,
        access_history_flattened.account_name,
        access_history_flattened.account_locator,
        {% endif %}
        access_history_flattened.table_id,
        access_history_flattened.object_name,
        access_history_flattened.object_domain,
        array_agg(distinct columns.value:columnName::text) as columns_accessed
    from access_history_flattened, lateral flatten(access_history_flattened.columns_array) as columns
    where
        access_history_flattened.object_name is not null
    group by all
)

select
    md5(concat(query_id, object_name)) as _unique_id,
    *
from access_history_flattened_w_columns
order by query_start_time asc
