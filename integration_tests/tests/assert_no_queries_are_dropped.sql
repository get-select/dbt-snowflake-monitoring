/*
We expect all queries in query_history to be present in cost_per_query, and vice versa.

Cost per query won't have some of the more recent queries, so we only compare queries
before the max(start_time) in cost_per_query
*/
WITH max_date AS (
    SELECT max(start_time) AS latest_start_time
    FROM {{ ref('cost_per_query') }}
)
SELECT *
FROM {{ ref('query_history') }} AS a
FULL OUTER JOIN {{ ref('cost_per_query') }} AS b
ON a.query_id=b.query_id
CROSS JOIN max_date
WHERE
    (
        a.query_id IS NULL
        OR b.query_id IS NULL
    )
    AND DATE(COALESCE(a.start_time, b.start_time)) < max_date.latest_start_time
