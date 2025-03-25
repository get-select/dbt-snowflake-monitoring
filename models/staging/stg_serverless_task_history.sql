{{ config(
    materialized='incremental',
    unique_key=['start_time', 'task_id'],
    enabled=not(var('uses_org_view', false))
) }}
-- This model is temporary disabled for Organisation Account views until Snowflake includes serverless_task_history
-- in the Organisation Account. They are planning this for Q2 (May - Jul) 2025.
select
    start_time,
    end_time,
    task_id,
    task_name,
    schema_id,
    schema_name,
    database_id,
    database_name,
    credits_used
from {{ source('snowflake_account_usage', 'serverless_task_history') }}

{% if is_incremental() %}
    where end_time > (select dateadd(day, -3, coalesce(max(end_time), '1970-01-01') ) from {{ this }})
{% endif %}

order by start_time
