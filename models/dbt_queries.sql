{{ config(
    materialized='incremental',
    unique_key=['query_id', 'start_time']
) }}

select
    dbt_metadata['dbt_snowflake_query_tags_version']::string as dbt_snowflake_query_tags_version, -- this will be null where the metadata came from a query comment in dbt-snowflake-monitoring versions <2.0.0
    dbt_metadata['invocation_id']::string as dbt_invocation_id,
    dbt_metadata['node_id']::string as dbt_node_id,
    dbt_metadata['node_resource_type']::string as dbt_node_resource_type,
    coalesce(dbt_metadata['node_name']::string, replace(array_slice(split(dbt_node_id, '.'), -1, array_size(split(dbt_node_id, '.')))[0], '"')) as dbt_node_name, -- we can just use node_name once enough time has been that users have migrated to v2.0.0
    dbt_metadata['materialized']::string as dbt_node_materialized,
    dbt_metadata['is_incremental']::boolean as dbt_node_is_incremental,
    dbt_metadata['node_alias']::string as dbt_node_alias,
    dbt_metadata['node_meta']::variant as dbt_node_meta,
    dbt_metadata['node_tags']::array as node_tags,
    iff(dbt_snowflake_query_tags_version >= '1.1.3', dbt_metadata['node_refs']::array, []) as dbt_node_refs, -- correct refs available from 1.1.3 onwards
    dbt_metadata['node_database']::string as dbt_node_database,
    dbt_metadata['node_schema']::string as dbt_node_schema,
    dbt_metadata['dbt_version']::string as dbt_version,
    dbt_metadata['project_name']::string as dbt_project_name,
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
    case
        when dbt_cloud_project_id is not null
        then
            {% if var('dbt_cloud_account_id', none) -%}
            '{{ var('dbt_cloud_url', 'https://cloud.getdbt.com/deploy/') }}' || '{{ var('dbt_cloud_account_id') }}' || '/projects/' || dbt_cloud_project_id || '/jobs/' || dbt_cloud_job_id
            {%- else -%}
            'Required dbt_cloud_account_id variable not set' -- noqa
            {%- endif %}
    end as dbt_cloud_job_url,
    case
        when dbt_cloud_project_id is not null
        then
            {% if var('dbt_cloud_account_id', none) -%}
            '{{ var('dbt_cloud_url', 'https://cloud.getdbt.com/deploy/') }}' || '{{ var('dbt_cloud_account_id') }}' || '/projects/' || dbt_cloud_project_id || '/runs/' || dbt_cloud_run_id
            {%- else -%}
            'Required dbt_cloud_account_id variable not set' -- noqa
            {%- endif %}
    end as dbt_cloud_run_url,
    * exclude dbt_metadata
from {{ ref('query_history_enriched') }}
where dbt_metadata is not null
    {% if is_incremental() %}
        -- Conservatively re-process the last 3 days to account for late arriving rates data which changes the cost per query. 
        -- Allow an override from project variable
        and end_time > (select dateadd(day, -{{ var('dbt_snowflake_monitoring_incremental_days', '3') }}, max(end_time)) from {{ this }})
    {% endif %}
