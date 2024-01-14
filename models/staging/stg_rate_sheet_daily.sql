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
        -- Have only seen this on one account. Normally it is COMPUTE, and all our downstream models rely on that
        -- May adjust this in the future if Snowflake is permanently changing these fields for all accounts
        when service_type='WAREHOUSE_METERING' then 'COMPUTE' 
        else service_type 
    end as service_type
from {{ source('snowflake_organization_usage', 'rate_sheet_daily') }}
order by date
