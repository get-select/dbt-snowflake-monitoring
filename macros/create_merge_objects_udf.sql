{% macro create_merge_objects_udf(relation) %}

create or replace function {{ adapter.quote_as_configured(this.database, 'database') }}.{{  adapter.quote_as_configured(this.schema, 'schema') }}.merge_objects(obj1 variant, obj2 variant)
returns variant
language javascript
comment = 'Created by dbt-snowflake-monitoring dbt package.'
as
$$
    return x = Object.assign(OBJ1, OBJ2)
$$

{% endmacro %}
