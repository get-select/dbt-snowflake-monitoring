-- <1000 rows, will be more expensive to materialize incrementally with multiple SQL statements
{{ config(materialized='table') }}

select
    date,
    organization_name,
    contract_number,
    account_name,
    account_locator,
    region,
    service_level,
    usage_type,
    currency,
    effective_rate,
    case
        -- For most Snowflake accounts, the service_type field is always COMPUTE or STORAGE
        -- Have recently seen new values introduced for one account: WAREHOUSE_METERING and CLOUD_SERVICES
        -- For now, we'll force these to either be COMPUTE or STORAGE since that's what the downstream models expect
        -- May adjust this in the future if Snowflake is permanently changing these fields for all accounts and starts offering different credit rates per usage_type
        when service_type in ('STORAGE', 'HYBRID_TABLE_STORAGE') then 'STORAGE'
        else 'COMPUTE'
    end as service_type
from {{ source('snowflake_organization_usage', 'rate_sheet_daily') }}
order by date
