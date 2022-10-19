{{ config(materialized='view') }}

select
    name,
    credits_used_compute,
    start_time,
    service_type,
    credits_used_cloud_services,
    credits_used
from {{ source('snowflake_account_usage', 'metering_history') }}
