{# Inspired by https://towardsdatascience.com/how-to-do-unit-testing-in-dbt-cb5fb660fbd8 #}

{% test advanced_equality(model, compare_model, round_columns=None) %}

{% set compare_columns = adapter.get_columns_in_relation(model) | map(attribute='quoted') %}
{% set compare_cols_csv = compare_columns | join(', ') %}

{% if round_columns %}
    {% set round_columns_enriched = [] %}
    {% for col in round_columns %}
        {% do round_columns_enriched.append('trunc('+col+', 8)') %}
    {% endfor %}
    {% set selected_columns = '* exclude(' + round_columns|join(', ') + "), " + round_columns_enriched|join(', ') %}
{% else %}
    {% set round_columns_csv = None %}
    {% set selected_columns = '*' %}
{% endif %}

with a as (
    select {{compare_cols_csv}} from {{ model }}
),
b as (
    select {{compare_cols_csv}} from {{ compare_model }}
),
a_minus_b as (
    select {{ selected_columns }} from a
    {{ except() }}
    select {{ selected_columns }} from b
),
b_minus_a as (
    select {{ selected_columns }} from b
    {{ except() }}
    select {{ selected_columns }} from a
),

unioned as (
    select 'in_actual_not_in_expected' as which_diff, a_minus_b.* from a_minus_b
    union all
    select 'in_expected_not_in_actual' as which_diff, b_minus_a.* from b_minus_a
)
select * from unioned
{% endtest %}
