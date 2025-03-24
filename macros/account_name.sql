{% macro account_name() %}
{%- if var('account_name', none) -%}
'{{ var('account_name') }}'
{%- else -%}
current_account_name()
{%- endif -%}
{%- endmacro %}
