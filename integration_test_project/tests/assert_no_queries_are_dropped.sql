/*
We expect all queries in query_history to be present in cost_per_query, and vice versa.

Cost per query won't have some of the more recent queries, so we only compare queries
before the max(start_time) in cost_per_query
*/
with max_date as (
    select max(start_time) as latest_start_time
    from {{ ref('cost_per_query') }}
)

select *
from {{ ref('query_history') }} as a
full outer join {{ ref('cost_per_query') }} as b
    on a.query_id = b.query_id
cross join max_date
where
    (
        a.query_id is null
        or b.query_id is null
    )
    and date(coalesce(a.start_time, b.start_time)) < max_date.latest_start_time
