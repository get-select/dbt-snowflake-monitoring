{{ config(materialized='table') }}

with
stop_threshold as (
    select max(start_time) as timestamp
    from {{ ref('stg_query_history') }}
),

warehouse_snapshots_base as (
    select
        warehouse_id,
        warehouse_size,
        warehouse_name,
        start_time as timestamp,
        lag(warehouse_size) over (partition by warehouse_id order by start_time) as prev_warehouse_size,
        lag(warehouse_name) over (partition by warehouse_id order by start_time) as prev_warehouse_name
    from {{ ref('stg_query_history') }}
    where
        warehouse_size is not null
),

warehouse_snapshots as (
    select
        warehouse_id,
        warehouse_name,
        warehouse_size,
        timestamp as valid_from,
        lead(timestamp) over (partition by warehouse_id order by timestamp) as _valid_to
    from warehouse_snapshots_base
    where
        warehouse_size != coalesce(prev_warehouse_size, '')
        or warehouse_name != coalesce(prev_warehouse_name, '')
)

select
    warehouse_snapshots.warehouse_id,
    warehouse_snapshots.warehouse_name,
    warehouse_snapshots.warehouse_size,
    warehouse_snapshots.valid_from,
    coalesce(warehouse_snapshots._valid_to, stop_threshold.timestamp) as valid_to,
    warehouse_snapshots._valid_to is null as is_current
from warehouse_snapshots
cross join stop_threshold
