{{ config(materialized='view') }}

select
    usage_date as date,
    average_stage_bytes
from {{ source('snowflake_account_usage', 'stage_storage_usage_history') }}
