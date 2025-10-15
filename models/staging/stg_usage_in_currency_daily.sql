select
    organization_name,
    contract_number,
    account_name,
    account_locator,
    region,
    service_level,
    usage_date,
    usage_type,
    currency,
    usage,
    usage_in_currency,
    balance_source,
    billing_type,
    rating_type,
    service_type,
    is_adjustment
from {{ source('snowflake_organization_usage', 'usage_in_currency_daily') }}
