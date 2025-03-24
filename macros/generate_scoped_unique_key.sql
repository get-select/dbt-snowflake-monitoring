{% macro generate_scoped_unique_key(base_fields) %}
    {% if var('uses_org_view', false) %}
        ['account_name', {{ base_fields|join(', ') }}]
    {% else %}
        {{ base_fields }}
    {% endif %}
{% endmacro %}
