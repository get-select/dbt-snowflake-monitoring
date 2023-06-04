select
    date,
    organization_name,
    currency,
    free_usage_balance,
    capacity_balance,
    on_demand_consumption_balance,
    rollover_balance
from {{ ref('stg_remaining_balance_daily') }}
{#
    From what I can tell, there will only ever be 1 organization_name in remaining_balance_daily.
    During a contract switchover, there may be two records with the same date, but different contract_numbers.
    Assume the higher contract_number is more recent. Chose not to group by date and aggregate balances in
    case the currency changes..
#}
qualify row_number() over (partition by date order by contract_number desc) = 1
