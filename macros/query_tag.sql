{% macro set_query_tag() -%}
    {# Start with any model-configured dict #}
    {% set tag_dict = config.get('query_tag', default={}) %}

    {# Regardless of resource type, we can always access the config via the 'model' variable #}
    {%- do tag_dict.update(
        dbt_snowflake_monitoring_version='1.6.2',
        app='dbt',
        dbt_version=dbt_version,
        project_name=project_name,
        target_name=target.name,
        target_database=target.database,
        target_schema=target.schema,
        invocation_id=invocation_id,
        node_name=model.name,
        node_alias=model.alias,
        node_package_name=model.package_name,
        node_original_file_path=model.original_file_path,
        node_database=model.database,
        node_schema=model.schema,
        node_id=model.unique_id,
        node_resource_type=model.resource_type,
        materialized=model.config.materialized,
        is_incremental=is_incremental(),
    ) -%}

    {# dbt Cloud stuff #}
    {%- if env_var('DBT_CLOUD_PROJECT_ID', False) -%}
        {%- do tag_dict.update(
            dbt_cloud_project_id=env_var('DBT_CLOUD_PROJECT_ID')
        ) -%}
    {%- endif -%}
    {%- if env_var('DBT_CLOUD_JOB_ID', False) -%}
        {%- do tag_dict.update(
            dbt_cloud_job_id=env_var('DBT_CLOUD_JOB_ID')
        ) -%}
    {%- endif -%}
    {%- if env_var('DBT_CLOUD_RUN_ID', False) -%}
        {%- do tag_dict.update(
            dbt_cloud_run_id=env_var('DBT_CLOUD_RUN_ID')
        ) -%}
    {%- endif -%}
    {%- if env_var('DBT_CLOUD_RUN_REASON_CATEGORY', False) -%}
        {%- do tag_dict.update(
            dbt_cloud_run_reason_category=env_var('DBT_CLOUD_RUN_REASON_CATEGORY')
        ) -%}
    {%- endif -%}
    {%- if env_var('DBT_CLOUD_RUN_REASON', False) -%}
        {%- do tag_dict.update(
            dbt_cloud_run_reason=env_var('DBT_CLOUD_RUN_REASON')
        ) -%}
    {%- endif -%}

    {% set new_query_tag = tojson(tag_dict) %}
    {% set original_query_tag = get_current_query_tag() %}
    {{ log("Setting query_tag to '" ~ new_query_tag ~ "'. Will reset to '" ~ original_query_tag ~ "' after materialization.") }}
    {% do run_query("alter session set query_tag = '{}'".format(new_query_tag)) %}
    {{ return(original_query_tag)}}
{% endmacro %}

{% macro unset_query_tag(original_query_tag) -%}
    {% if original_query_tag %}
    {{ log("Resetting query_tag to '" ~ original_query_tag ~ "'.") }}
    {% do run_query("alter session set query_tag = '{}'".format(original_query_tag)) %}
    {% else %}
    {{ log("No original query_tag, unsetting parameter.") }}
    {% do run_query("alter session unset query_tag") %}
    {% endif %}
{% endmacro %}
