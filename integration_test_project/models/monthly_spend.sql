select
    date_trunc(month, date) as month,
    service,
    sum(spend) as spend
from {{ 'daily_spend' }}
where date_trunc(month, date) = '2022-04-01' group by 1, 2
