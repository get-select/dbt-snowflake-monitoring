{# Have to reimplement is_incremental because node.type hasn't populated at this stage #}
{# This doesn't yet account for full refreshes due to table not existing yet #}
{% macro custom_is_incremental(node) %}
    {{ return(
        node.config.materialized == 'incremental'
        and not dbt_snowflake_monitoring.custom_should_full_refresh(node)
    ) }}
{% endmacro %}

{% macro custom_should_full_refresh(node) %}
  {% set config_full_refresh = node.config.get('full_refresh') %}
  {% if config_full_refresh is none %}
    {% set config_full_refresh = flags.FULL_REFRESH %}
  {% endif %}
  {% do return(config_full_refresh) %}
{% endmacro %}

{% macro get_query_comment(node) %}
{%- set comment_dict = {} -%}
{%- do comment_dict.update(
    dbt_snowflake_monitoring_version='1.6.2',
    app='dbt',
    dbt_version=dbt_version,
    target_name=target.name,
    target_database=target.database,
    target_schema=target.schema,
    invocation_id=invocation_id,
) -%}
{%- if env_var('DBT_CLOUD_PROJECT_ID', False) -%}
  {%- do comment_dict.update(
    dbt_cloud_project_id=env_var('DBT_CLOUD_PROJECT_ID')
  ) -%}
{%- endif -%}
{%- if env_var('DBT_CLOUD_JOB_ID', False) -%}
  {%- do comment_dict.update(
    dbt_cloud_job_id=env_var('DBT_CLOUD_JOB_ID')
  ) -%}
{%- endif -%}
{%- if env_var('DBT_CLOUD_RUN_ID', False) -%}
  {%- do comment_dict.update(
    dbt_cloud_run_id=env_var('DBT_CLOUD_RUN_ID')
  ) -%}
{%- endif -%}
{%- if env_var('DBT_CLOUD_RUN_REASON_CATEGORY', False) -%}
  {%- do comment_dict.update(
    dbt_cloud_run_reason_category=env_var('DBT_CLOUD_RUN_REASON_CATEGORY')
  ) -%}
{%- endif -%}
{%- if env_var('DBT_CLOUD_RUN_REASON', False) -%}
  {%- do comment_dict.update(
    dbt_cloud_run_reason=env_var('DBT_CLOUD_RUN_REASON')
  ) -%}
{%- endif -%}
{%- if node is not none -%}
  {%- do comment_dict.update(
    node_name=node.name,
    node_alias=node.alias,
    node_package_name=node.package_name,
    node_original_file_path=node.original_file_path,
    node_database=node.database,
    node_schema=node.schema,
    node_id=node.unique_id,
    node_resource_type=node.resource_type
  ) -%}
  {%- if node.resource_type == 'model' -%}
  {%- do comment_dict.update(
    materialized=node.config.materialized,
    is_incremental=dbt_snowflake_monitoring.custom_is_incremental(node)
  ) -%}
  {%- endif -%}
{%- endif -%}
{{ return(tojson(comment_dict)) }}
{% endmacro %}
