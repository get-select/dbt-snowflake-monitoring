{{ config(materialized='view') }}

select
    usage_date as date,
    storage_bytes,
    stage_bytes,
    failsafe_bytes,
    hybrid_table_storage_bytes,
    archive_storage_cool_bytes,
    archive_storage_cold_bytes,
    archive_storage_retrieval_temp_bytes
from {{ source('snowflake_account_usage', 'storage_usage') }}
