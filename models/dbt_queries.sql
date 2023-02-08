{{ config(
    materialized='incremental',
    unique_key=['query_id', 'start_time']
) }}

select
    dbt_metadata['invocation_id']::string as dbt_invocation_id,
    dbt_metadata['node_id']::string as dbt_node_id,
    dbt_metadata['node_resource_type']::string as dbt_node_resource_type,
    coalesce(dbt_metadata['node_name']::string, replace(array_slice(split(dbt_node_id, '.'), -1, array_size(split(dbt_node_id, '.')))[0], '"')) as dbt_node_name, -- we can just use node_name once enough time has been that users have migrated to v2.0.0
    dbt_metadata['materialized']::string as dbt_node_materialized,
    dbt_metadata['is_incremental']::string as dbt_node_is_incremental,
    dbt_metadata['node_alias']::string as dbt_node_alias,
    dbt_metadata['node_database']::string as dbt_node_database,
    dbt_metadata['node_schema']::string as dbt_node_schema,
    dbt_metadata['dbt_version']::string as dbt_version,
    dbt_metadata['target_name']::string as dbt_target_name,
    dbt_metadata['target_database']::string as dbt_target_database,
    dbt_metadata['target_schema']::string as dbt_target_schema,
    dbt_metadata['node_package_name']::string as dbt_node_package_name,
    dbt_metadata['node_original_file_path']::string as dbt_node_original_file_path,
    dbt_metadata['dbt_cloud_project_id']::string as dbt_cloud_project_id,
    dbt_metadata['dbt_cloud_job_id']::string as dbt_cloud_job_id,
    dbt_metadata['dbt_cloud_run_id']::string as dbt_cloud_run_id,
    dbt_metadata['dbt_cloud_run_reason_category']::string as dbt_cloud_run_reason_category,
    dbt_metadata['dbt_cloud_run_reason']::string as dbt_cloud_run_reason,
    min(start_time) over (partition by dbt_invocation_id, dbt_node_id order by start_time asc) as node_start_time,
    dbt_metadata['dbt_snowflake_query_tags_version']::string as dbt_snowflake_query_tags_version, -- this will be null where the metadata came from a query comment in dbt-snowflake-monitoring versions <2.0.0
    {% if var('dbt_cloud_account_id', none) -%}
    'https://cloud.getdbt.com/next/deploy/' || '{{ var('dbt_cloud_account_id') }}' || '/projects/' || dbt_cloud_project_id || '/jobs/' || dbt_cloud_job_id as dbt_cloud_job_url,
    'https://cloud.getdbt.com/next/deploy/' || '{{ var('dbt_cloud_account_id') }}' || '/projects/' || dbt_cloud_project_id || '/runs/' || dbt_cloud_run_id as dbt_cloud_run_url,
    {%- else -%}
    'Required dbt_cloud_account_id variable not set' as dbt_cloud_job_url, -- noqa
    'Required dbt_cloud_account_id variable not set' as dbt_cloud_run_url,
    {%- endif %}
    * exclude dbt_metadata
from {{ ref('query_history_enriched') }}
where dbt_metadata is not null
    {% if is_incremental() %}
        -- Conservatively re-process the last 7 days to account for late arriving rates data
        -- which changes the cost per query
        and end_time > (select dateadd(day, -7, max(end_time)) from {{ this }})
    {% endif %}
