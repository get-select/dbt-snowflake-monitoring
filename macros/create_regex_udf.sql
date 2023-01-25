{% macro create_regexp_replace_udf() %}

create or replace function {{ target.database }}.{{ target.schema }}.dbt_snowflake_monitoring_regexp_replace(subject text, pattern text, replacement text)
returns string
language javascript
comment = 'Created by dbt-snowflake-monitoring dbt package.'
as
$$
    const p = SUBJECT;
    let regex = new RegExp(PATTERN, 'g')
    return p.replace(regex, REPLACEMENT);
$$

{% endmacro %}
