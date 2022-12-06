{{ config(materialized='incremental') }}

select
    start_time,
    end_time,
    task_id,
    task_name,
    database_name,
    credits_used
from {{ source('snowflake_account_usage', 'serverless_task_history') }}

{% if is_incremental() %}
    where end_time > (select max(end_time) from {{ this }})
{% endif %}

order by start_time
