{{ config(materialized='view') }}

select
    {{ add_account_columns() }}
    usage_date as date,
    database_name,
    average_database_bytes,
    average_failsafe_bytes,
    average_hybrid_table_storage_bytes
from {{ source('snowflake_account_usage', 'database_storage_usage_history') }}
