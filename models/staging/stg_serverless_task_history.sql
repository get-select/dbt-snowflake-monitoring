{{ config(
    materialized='incremental',
    unique_key=['start_time', 'task_id'],
    )
}}

select
    start_time,
    end_time,
    task_id,
    task_name,
    database_name,
    credits_used
from {{ source('snowflake_account_usage', 'serverless_task_history') }}

{% if is_incremental() %}
    where end_time > (select dateadd(day, -3, coalesce(max(end_time), '1970-01-01') ) from {{ this }})
{% endif %}

order by start_time
