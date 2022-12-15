{{ config(materialized='table') }}

with
stop_threshold as (
    select max(timestamp) as timestamp
    from {{ ref('stg_warehouse_events_history') }}
),

warehouse_cluster_status_base as (
    select
        warehouse_id,
        warehouse_name,
        cluster_number + 1 as cluster_number,
        timestamp as valid_from,
        lead(timestamp) over (partition by warehouse_id, cluster_number order by timestamp asc) as valid_to,
        event_name = 'RESUME_CLUSTER' as is_active
    from {{ ref('stg_warehouse_events_history') }}
    where
        event_name in ('RESUME_CLUSTER', 'SUSPEND_CLUSTER')
        and event_state = 'COMPLETED'
),

warehouse_cluster_status as (
    select
        warehouse_cluster_status_base.warehouse_id,
        warehouse_cluster_status_base.warehouse_name,
        warehouse_cluster_status_base.cluster_number,
        warehouse_cluster_status_base.is_active,
        warehouse_cluster_status_base.valid_from,
        coalesce(warehouse_cluster_status_base.valid_to, stop_threshold.timestamp) as valid_to
    from warehouse_cluster_status_base
    cross join stop_threshold
)

select *
from warehouse_cluster_status
order by valid_from
