{{ config(materialized='view') }}

select
    {% if var('uses_org_view', false) %}
    organization_name,
    account_name,
    account_locator,
    {% endif %}
    usage_date as date,
    database_name,
    average_database_bytes,
    average_failsafe_bytes,
    average_hybrid_table_storage_bytes
from {{ source('snowflake_account_usage', 'database_storage_usage_history') }}
