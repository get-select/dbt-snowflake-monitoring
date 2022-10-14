{{ config(materialized='table') }}

WITH
daily_rates_base AS (
    SELECT
        date,
        effective_rate,
        MAX(date) OVER() AS latest_date
    FROM {{ ref('rate_sheet_daily') }}
    WHERE usage_type='compute'
),
/*
Some days have more than 1 rate, likely due to a new contract. Just pick the higher rate for that day.
*/
daily_rates_deduped AS (
    SELECT
        date,
        GET(ARRAY_AGG(effective_rate) WITHIN GROUP (ORDER BY effective_rate DESC), 0) AS effective_rate,
        ANY_VALUE(latest_date) AS latest_date
    FROM daily_rates_base
    GROUP BY 1
),
/*
rate_sheet_daily will always be behind by 1 day
to avoid losing queries from today when we inner join, we will
add a new record to rate sheet daily for the missing day, using
the most recent effective_rate
*/
daily_rates AS (
    SELECT
        date,
        effective_rate
    FROM daily_rates_deduped

    UNION ALL

    SELECT
        DATEADD('day', 1, latest_date) AS date,
        effective_rate
    FROM daily_rates_deduped
    WHERE
        date = latest_date
),
/*
Calculate a "stop threshold", which tells us the latest timestamp we should process data up until.

For example, if warehouse metering history has data up until 2022-10-10 16:00:00, daily_rates
has data for 2022-10-10 (aka the full day), then we would only want to consider queries
that ended before 2022-10-10 16:00:00. Otherwise we won't accurately calculate their cost.
*/
stop_thresholds AS (
    SELECT MAX(end_time) AS latest_ts
    FROM {{ ref('warehouse_metering_history') }}

    UNION ALL

    -- Can use data up until the end of the day of the latest date in daily_rates
    SELECT DATEADD('day', 1, MAX(date)) AS latest_ts
    FROM daily_rates
),
stop_threshold AS (
    SELECT MIN(latest_ts) AS latest_ts
    FROM stop_thresholds
),
filtered_queries AS (
    SELECT
        query_id,
        query_text AS original_query_text,
        credits_used_cloud_services,
        warehouse_id,
        warehouse_size IS NOT NULL AS ran_on_warehouse,
        TIMEADD(
            'millisecond',
            queued_overload_time + compilation_time +
            queued_provisioning_time + queued_repair_time +
            list_external_files_time,
            start_time
        ) AS execution_start_time,
        start_time,
        end_time
    FROM {{ ref('query_history') }} AS q
    CROSS JOIN stop_threshold AS st
    WHERE
        q.end_time <= st.latest_ts
),
hours_list AS (
    SELECT
        DATEADD(
            'hour',
            '-' || row_number() over (order by null),
            DATEADD('day', '+1', CURRENT_DATE)
        ) as hour_start,
        DATEADD('hour', '+1', hour_start) AS hour_end
    FROM TABLE(generator(rowcount => (24*730))) t
),
-- 1 row per hour a query ran
query_hours AS (
    SELECT
        hl.hour_start,
        hl.hour_end,
        queries.*
    FROM hours_list AS hl
    INNER JOIN filtered_queries AS queries
        ON hl.hour_start >= DATE_TRUNC('hour', queries.execution_start_time)
        AND hl.hour_start < queries.end_time
        AND queries.ran_on_warehouse
),
query_seconds_per_hour AS (
    SELECT
        *,
        DATEDIFF('millisecond', GREATEST(execution_start_time, hour_start), LEAST(end_time, hour_end)) AS num_milliseconds_query_ran,
        SUM(num_milliseconds_query_ran) OVER (PARTITION BY warehouse_id, hour_start) AS total_query_milliseconds_in_hour,
        num_milliseconds_query_ran/total_query_milliseconds_in_hour AS fraction_of_total_query_time_in_hour,
        hour_start AS hour
    FROM query_hours
),
credits_billed_hourly AS (
    SELECT
        start_time AS hour,
        warehouse_id,
        credits_used_compute,
        credits_used_cloud_services
    FROM {{ ref('warehouse_metering_history') }}
),
query_cost AS (
    SELECT
        query.*,
        credits.credits_used_compute*dr.effective_rate AS actual_warehouse_cost,
        credits.credits_used_compute*fraction_of_total_query_time_in_hour*dr.effective_rate AS allocated_compute_cost_in_hour
    FROM query_seconds_per_hour AS query
    INNER JOIN credits_billed_hourly AS credits
        ON query.warehouse_id=credits.warehouse_id
        AND query.hour=credits.hour
    INNER JOIN daily_rates AS dr
        ON DATE(query.start_time)=dr.date
),
cost_per_query AS (
    SELECT
        query_id,
        ANY_VALUE(start_time) AS start_time,
        SUM(allocated_compute_cost_in_hour) AS compute_cost,
        ANY_VALUE(credits_used_cloud_services) AS credits_used_cloud_services
    FROM query_cost
    GROUP BY 1
),
credits_billed_daily AS (
    SELECT
        DATE(hour) AS date,
        SUM(credits_used_compute) AS daily_credits_used_compute,
        SUM(credits_used_cloud_services) AS daily_credits_used_cloud_services,
        GREATEST(daily_credits_used_cloud_services - daily_credits_used_compute*0.1, 0) AS daily_billable_cloud_services
    FROM credits_billed_hourly
    GROUP BY 1
),
all_queries AS (
    SELECT
        query_id,
        start_time,
        compute_cost,
        credits_used_cloud_services
    FROM cost_per_query

    UNION ALL

    SELECT
        query_id,
        start_time,
        0 AS compute_cost,
        credits_used_cloud_services
    FROM filtered_queries
    WHERE
        NOT ran_on_warehouse
)
SELECT
    q.query_id,
    q.start_time,
    q.compute_cost,
    (q.credits_used_cloud_services/credits.daily_credits_used_cloud_services*daily_billable_cloud_services)*dr.effective_rate AS cloud_services_cost,
    q.compute_cost + cloud_services_cost AS query_cost
FROM all_queries AS q
INNER JOIN credits_billed_daily AS credits
    ON DATE(q.start_time)=credits.date
INNER JOIN daily_rates AS dr
    ON DATE(q.start_time)=dr.date
ORDER BY q.start_time