{% macro add_account_columns() %}
    {% if var('uses_org_view', false) %}
    organization_name,
    account_name,
    account_locator,
    {% else %}
    current_organization_name() as organization_name,
    current_account_name() as account_name,
    current_account() as account_locator,
    {% endif %}
{% endmacro %}
