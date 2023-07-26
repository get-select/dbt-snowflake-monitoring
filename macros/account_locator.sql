{% macro account_locator() %}
{%- if var('account_locator', none) -%}
'{{ var('account_locator') }}'
{%- else -%}
current_account()
{%- endif -%}
{%- endmacro %}
