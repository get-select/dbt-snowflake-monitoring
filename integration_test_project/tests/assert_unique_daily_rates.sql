select
    date,
    usage_type
from {{ ref('daily_rates') }}
group by 1,2
having count(*) > 1