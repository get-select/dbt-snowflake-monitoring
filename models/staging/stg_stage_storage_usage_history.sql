{{ config(materialized='view') }}

select
    {{ add_account_columns() }}
    usage_date as date,
    average_stage_bytes
from {{ source('snowflake_account_usage', 'stage_storage_usage_history') }}
