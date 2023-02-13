{% macro set_query_tag() -%}
    {{ return(adapter.dispatch('set_query_tag', 'dbt_snowflake_query_tags')()) }}
{%- endmacro %}

{% macro default__set_query_tag() -%}
    {{ return(adapter.dispatch('set_query_tag', 'dbt_snowflake_query_tags')()) }}
{% endmacro %}

{% macro unset_query_tag(original_query_tag) -%}
    {{ return(adapter.dispatch('unset_query_tag', 'dbt_snowflake_query_tags')(original_query_tag)) }}
{%- endmacro %}

{% macro default__unset_query_tag(original_query_tag) -%}
    {{ return(adapter.dispatch('unset_query_tag', 'dbt_snowflake_query_tags')(original_query_tag)) }}
{% endmacro %}
