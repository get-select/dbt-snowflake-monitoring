{{ config(materialized='view') }}

select
    {% if var('uses_org_view', false) %}
    organization_name,
    account_name,
    account_locator,
    {% endif %}
    usage_date as date,
    average_stage_bytes
from {{ source('snowflake_account_usage', 'stage_storage_usage_history') }}
