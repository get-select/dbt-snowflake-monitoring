select
    start_time,
    end_time,
    source_cloud,
    source_region,
    target_cloud,
    target_region,
    bytes_transferred,
    transfer_type
from {{ source('snowflake_account_usage', 'data_transfer_history')}}
