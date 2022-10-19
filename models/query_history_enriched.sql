{{ config(materialized='incremental') }}

with
query_history as (
    select
        *,

        -- this removes comments enclosed by /* <comment text> */
        regexp_replace(query_text, '(/\*.*\*/)') as _query_text_no_comments,
        -- this removes single line comments starting with -- and either ending with a new line or end of string
        regexp_replace(_query_text_no_comments, '(--.*$)|(--.*\n)') as query_text_no_comments,

        regexp_substr(query_text, '/\\*\\s({"app":\\s"dbt".*})\\s\\*/', 1, 1, 'ie') as _dbt_json_meta,
        try_parse_json(_dbt_json_meta) as dbt_metadata

    from {{ ref('stg_query_history') }}

    {% if is_incremental() %}
        where end_time > (select max(end_time) from {{ this }})
    {% endif %}
),

cost_per_query as (
    select *
    from {{ ref('cost_per_query') }}
    {% if is_incremental() %}
        where end_time > (select max(end_time) from {{ this }})
    {% endif %}
)

select
    cost_per_query.query_id,
    cost_per_query.compute_cost,
    cost_per_query.cloud_services_cost,
    cost_per_query.query_cost,
    cost_per_query.execution_start_time,

    -- Grab all columns from query_history (except the query time columns which we rename below)
    query_history.query_text,
    query_history.database_id,
    query_history.database_name,
    query_history.schema_id,
    query_history.schema_name,
    query_history.query_type,
    query_history.session_id,
    query_history.user_name,
    query_history.role_name,
    query_history.warehouse_id,
    query_history.warehouse_name,
    query_history.warehouse_size,
    query_history.warehouse_type,
    query_history.cluster_number,
    query_history.query_tag,
    query_history.execution_status,
    query_history.error_code,
    query_history.error_message,
    query_history.start_time,
    query_history.end_time,
    query_history.total_elapsed_time,
    query_history.bytes_scanned,
    query_history.percentage_scanned_from_cache,
    query_history.bytes_written,
    query_history.bytes_written_to_result,
    query_history.bytes_read_from_result,
    query_history.rows_produced,
    query_history.rows_inserted,
    query_history.rows_updated,
    query_history.rows_deleted,
    query_history.rows_unloaded,
    query_history.bytes_deleted,
    query_history.partitions_scanned,
    query_history.partitions_total,
    query_history.bytes_spilled_to_local_storage,
    query_history.bytes_spilled_to_remote_storage,
    query_history.bytes_sent_over_the_network,
    query_history.outbound_data_transfer_cloud,
    query_history.outbound_data_transfer_region,
    query_history.outbound_data_transfer_bytes,
    query_history.inbound_data_transfer_cloud,
    query_history.inbound_data_transfer_region,
    query_history.inbound_data_transfer_bytes,
    query_history.credits_used_cloud_services,
    query_history.release_version,
    query_history.external_function_total_invocations,
    query_history.external_function_total_sent_rows,
    query_history.external_function_total_received_rows,
    query_history.external_function_total_sent_bytes,
    query_history.external_function_total_received_bytes,
    query_history.query_load_percent,
    query_history.is_client_generated_statement,
    query_history.query_acceleration_bytes_scanned,
    query_history.query_acceleration_partitions_scanned,
    query_history.query_acceleration_upper_limit_scale_factor,

    -- Rename some existing columns for clarity
    query_history.total_elapsed_time as total_elapsed_time_ms,
    query_history.compilation_time as compilation_time_ms,
    query_history.queued_provisioning_time as queued_provisioning_time_ms,
    query_history.queued_repair_time as queued_repair_time_ms,
    query_history.queued_overload_time as queued_overload_time_ms,
    query_history.transaction_blocked_time as transaction_blocked_time_ms,
    query_history.list_external_files_time as list_external_files_time_ms,
    query_history.execution_time as execution_time_ms,

    -- New columns
    query_history.warehouse_size is not null as ran_on_warehouse,
    query_history.bytes_scanned / power(1024, 3) as data_scanned_gb,
    data_scanned_gb * query_history.percentage_scanned_from_cache as data_scanned_from_cache_gb,
    query_history.bytes_spilled_to_local_storage / power(1024, 3) as data_spilled_to_local_storage_gb,
    query_history.bytes_spilled_to_remote_storage / power(1024, 3) as data_spilled_to_remote_storage_gb,
    query_history.bytes_sent_over_the_network / power(1024, 3) as data_sent_over_the_network_gb,
    query_history.query_text_no_comments,
    query_history.dbt_metadata,

    query_history.total_elapsed_time / 1000 as total_elapsed_time_s,
    query_history.compilation_time / 1000 as compilation_time_s,
    query_history.queued_provisioning_time / 1000 as queued_provisioning_time_s,
    query_history.queued_repair_time / 1000 as queued_repair_time_s,
    query_history.queued_overload_time / 1000 as queued_overload_time_s,
    query_history.transaction_blocked_time / 1000 as transaction_blocked_time_s,
    query_history.list_external_files_time / 1000 as list_external_files_time_s,
    query_history.execution_time / 1000 as execution_time_s

from query_history
inner join cost_per_query
    on query_history.query_id = cost_per_query.query_id
order by query_history.start_time
