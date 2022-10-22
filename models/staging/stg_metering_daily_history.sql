{{ config(materialized='view') }}

select
    usage_date as date,
    credits_adjustment_cloud_services
from {{ source('snowflake_account_usage', 'metering_daily_history') }}
