select
    dbt_metadata['dbt_version']::string as dbt_version,
    dbt_metadata['target_name']::string as dbt_target_name,
    dbt_metadata['target_database']::string as dbt_target_database,
    dbt_metadata['target_schema']::string as dbt_target_schema,
    dbt_metadata['invocation_id']::string as dbt_invocation_id,
    dbt_metadata['node_id']::string as dbt_node_id,
    dbt_metadata['node_resource_type']::string as dbt_node_resource_type,
    dbt_metadata['materialized']::string as dbt_node_materialized,
    dbt_metadata['is_incremental']::string as dbt_node_is_incremental,
    dbt_metadata['dbt_cloud_project_id']::string as dbt_cloud_project_id,
    dbt_metadata['dbt_cloud_job_id']::string as dbt_cloud_job_id,
    dbt_metadata['dbt_cloud_run_id']::string as dbt_cloud_run_id,
    dbt_metadata['dbt_cloud_run_reason_category']::string as dbt_cloud_run_reason_category,
    dbt_metadata['dbt_cloud_run_reason']::string as dbt_cloud_run_reason,
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
