# Introduction

The following sample queries are useful starting points for performing analysis on top of the models provided by this package. Before running the queries, make sure that the default database and schema have been set:

```sql
use database [dbt-snowflake-monitoring model database]
use schema [dbt-snowflake-monitoring model schema]
```

# Sample Queries

## Reproducing the Monthly Snowflake Invoice
The `daily_spend` model reproduces the monthly Snowflake invoice precisely, broken down by the same service categories. The `daily_spend` model contains additional columns for `warehouse_name`, `database_name` and `storage_type` to further break down spend (see more examples below).

```sql
select
    date_trunc(month, date)::date as month,
    service,
    sum(spend)
from daily_spend
group by 1, 2
```

## Warehouse Costs

### Monthly Spend per Warehouse
This query uses the `daily_spend` model to explore spend by warehouse name. Some queries don't require a running warehouse, which is why you'll see a row for `warehouse_name` = 'Cloud Services Only'.

```sql
select
    date_trunc(month, date)::date as month,
    service,
    warehouse_name,
    sum(spend)
from daily_spend
where service in ('Compute', 'Cloud Services', 'Adj For Incl Cloud Services')
group by 1, 2, 3
```

## Storage Costs

### Monthly Spend per Database
This query uses the `daily_spend` model to explore table spend by database. Note that this also returns storage costs for data held in stages (user and table stages or internal named stages), see the `storage_type` column.

```sql
select
    date_trunc(month, date)::date as month,
    service,
    storage_type,
    database_name,
    sum(spend)
from daily_spend
where service in ('Storage')
group by 1, 2, 3, 4
```

### Unused Tables
This query uses the `query_tables_accessed` model (available as of 1.6.0) to identify tables which have not been queried in the last 30 days. The total storage costs of each table is also shown, which leverages your Snowflake account's current storage rate (usually between $20-$40 per TB per month).

```sql
with
table_access_summary as (
    select
        full_table_name,
        count_if(query_start_time >= dateadd('day', -30, current_date)) as num_queries_last_30d,
        count_if(query_start_time >= dateadd('day', -90, current_date)) as num_queries_last_90d
    from query_tables_accessed
    group by 1
),
table_storage_metrics as (
    select
        table_catalog || '.' || table_schema || '.' || table_name as full_table_name,
        sum(active_bytes)/power(1024,3) as active_gb,
        sum(time_travel_bytes)/power(1024,3) as time_travel_gb,
        sum(failsafe_bytes)/power(1024,3) as failsafe_gb,
        sum(retained_for_clone_bytes)/power(1024,3) as retained_for_clone_gb,
        (active_gb + time_travel_gb + failsafe_gb + retained_for_clone_gb) as total_storage_gb,
        total_storage_gb/1024 as total_storage_tb,
        total_storage_tb*12*any_value(daily_rates.effective_rate) as annualized_storage_cost
    from snowflake.account_usage.table_storage_metrics
    inner join daily_rates
        on daily_rates.is_latest_rate
        and daily_rates.usage_type='storage'
    where
        not deleted
    group by 1
    order by annualized_storage_cost desc
)
select
    table_storage_metrics.*,
    table_access_summary.* exclude (full_table_name)
from table_storage_metrics
inner join table_access_summary
    on table_storage_metrics.full_table_name=table_access_summary.full_table_name
where
    num_queries_last_30d = 0 -- modify as needed
order by total_storage_gb desc
```

## Query Cost Attribution
Snowflake bills for the number of seconds that a warehouse is running, not by query. Query cost attribution helps understand how queries are contributing to warehouse active time. Removing a query will not reduce the bill by the exact amount attributed to the query if other queries are running at the same time and causing the warehouse to stay active.

Cloud services credits are only billed if they exceed 10% of the compute credit consumption on a given day. The cost per query model accounts for this, but current day values will change up until the end of the day.


### Top 10 costliest queries in the last 30 days

```sql
with
max_date as (
    select max(date(end_time)) as date
    from query_history_enriched
)
select
    md5(query_history_enriched.query_text_no_comments) as query_signature,
    any_value(query_history_enriched.query_text) as query_text,
    sum(query_history_enriched.query_cost) as total_cost_last_30d,
    total_cost_last_30d*12 as estimated_annual_cost,
    get(array_agg(warehouse_name) within group (order by start_time desc), 0)::string as latest_warehouse_name,
    get(array_agg(warehouse_size) within group (order by start_time desc), 0)::string as latest_warehouse_size,
    get(array_agg(query_id) within group (order by start_time desc), 0)::string as latest_query_id,
    avg(execution_time_s) as avg_execution_time_s,
    count(*) as num_executions
from query_history_enriched
cross join max_date
where
    query_history_enriched.start_time >= dateadd('day', -30, max_date.date)
    and query_history_enriched.start_time < max_date.date -- don't include partial day of data
group by 1
order by total_cost_last_30d desc
limit 10
```

## dbt Cost Attribution

### Top 10 costliest dbt models in the last 30 days

```sql
with
max_date as (
    select max(date(end_time)) as date
    from dbt_queries
)
select
    dbt_queries.dbt_node_id,
    sum(dbt_queries.query_cost) as total_cost_last_30d,
    total_cost_last_30d*12 as estimated_annual_cost
from dbt_queries
cross join max_date
where
    dbt_queries.start_time >= dateadd('day', -30, max_date.date)
    and dbt_queries.start_time < max_date.date -- don't include partial day of data
group by 1
order by total_cost_last_30d desc
limit 10
```

### Daily dbt model running costs

```sql
select
    date(start_time) as date,
    sum(query_cost) as cost
from dbt_queries
where dbt_node_id = '<dbt model node id>'
group by 1
order by 1 desc
```
