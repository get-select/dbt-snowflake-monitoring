/*
We expect all queries in query_history_enriched to be present in cost_per_query, and vice versa.

Cost per query won't have some of the more recent queries, so we only compare queries
before the current date
*/
select *
from {{ ref('query_history_enriched') }} as a
full outer join {{ ref('cost_per_query') }} as b
    on a.query_id = b.query_id
where
    (
        a.query_id is null
        or b.query_id is null
    )
    and date(coalesce(a.start_time, b.start_time)) < current_date
