{{ config(
    materialized='incremental',
    unique_key=['account_locator', 'query_id', 'start_time'] if var('uses_org_view', false) else ['query_id', 'start_time'],
) }}

select
    {% if var('uses_org_view', false) %}
    organization_name,
    account_name,
    account_locator,
    {% endif %}
    query_id,
    query_text,
    database_id,
    database_name,
    schema_id,
    schema_name,
    query_type,
    session_id,
    user_name,
    role_name,
    warehouse_id,
    warehouse_name,
    warehouse_size,
    warehouse_type,
    cluster_number,
    query_tag,
    execution_status,
    error_code,
    error_message,
    start_time,
    end_time,
    total_elapsed_time,
    bytes_scanned,
    percentage_scanned_from_cache,
    bytes_written,
    bytes_written_to_result,
    bytes_read_from_result,
    rows_produced,
    rows_inserted,
    rows_updated,
    rows_deleted,
    rows_unloaded,
    bytes_deleted,
    partitions_scanned,
    partitions_total,
    bytes_spilled_to_local_storage,
    bytes_spilled_to_remote_storage,
    bytes_sent_over_the_network,
    compilation_time,
    execution_time,
    queued_provisioning_time,
    queued_repair_time,
    queued_overload_time,
    transaction_blocked_time,
    outbound_data_transfer_cloud,
    outbound_data_transfer_region,
    outbound_data_transfer_bytes,
    inbound_data_transfer_cloud,
    inbound_data_transfer_region,
    inbound_data_transfer_bytes,
    list_external_files_time,
    credits_used_cloud_services,
    release_version,
    external_function_total_invocations,
    external_function_total_sent_rows,
    external_function_total_received_rows,
    external_function_total_sent_bytes,
    external_function_total_received_bytes,
    query_load_percent,
    is_client_generated_statement,
    query_acceleration_bytes_scanned,
    query_acceleration_partitions_scanned,
    query_acceleration_upper_limit_scale_factor,
    query_hash,
    query_hash_version,
    query_parameterized_hash,
    query_parameterized_hash_version,
    query_retry_time,
    query_retry_cause,
    fault_handling_time
from {{ source('snowflake_account_usage', 'query_history') }}

{% if is_incremental() %}
    -- must use end time in case query hasn't completed
    -- add lookback window of 2 days to account for late arriving queries
    where end_time > (select dateadd(day, -{{ var('dbt_snowflake_monitoring_incremental_days', '2') }}, coalesce(max(end_time), '1970-01-01') ) from {{ this }})
{% endif %}

order by start_time
