{% macro get_query_comment(node) %}
    {{ return(dbt_snowflake_query_tags.get_query_comment(node)) }}
{% endmacro %}
