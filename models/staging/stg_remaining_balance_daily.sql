-- <1000 rows, will be more expensive to materialize incrementally with multiple SQL statements
{{ config(materialized='table') }}

select
    date,
    organization_name,
    contract_number,
    currency,
    free_usage_balance,
    capacity_balance,
    on_demand_consumption_balance,
    rollover_balance
from {{ source('snowflake_organization_usage', 'remaining_balance_daily') }}
order by date
