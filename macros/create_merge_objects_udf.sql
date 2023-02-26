{% macro create_merge_objects_udf(relation) %}

create or replace function {{ relation.database }}.{{ relation.schema }}.merge_objects(obj1 variant, obj2 variant)
returns variant
language javascript
comment = 'Created by dbt-snowflake-monitoring dbt package.'
as
$$
    return x = Object.assign(OBJ1, OBJ2)
$$

{% endmacro %}
