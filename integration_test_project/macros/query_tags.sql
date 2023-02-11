{% macro set_query_tag() -%}
{% do return(dbt_snowflake_monitoring.set_query_tag()) %}
{% endmacro %}

{% macro unset_query_tag(original_query_tag) -%}
{% do return(dbt_snowflake_monitoring.unset_query_tag(original_query_tag)) %}
{% endmacro %}
