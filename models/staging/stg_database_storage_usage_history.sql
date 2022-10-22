{{ config(materialized='view') }}

select
    usage_date as date,
    database_name,
    average_database_bytes,
    average_failsafe_bytes
from {{ source('snowflake_account_usage', 'database_storage_usage_history') }}
