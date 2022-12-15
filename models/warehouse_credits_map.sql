{{ config(materialized='view') }}

with
warehouse_credits_map as (
    select * from (
        values
        ('X-Small', 'STANDARD', 1),
        ('Small', 'STANDARD', 2),
        ('Medium', 'STANDARD', 4),
        ('Large', 'STANDARD', 8),
        ('X-Large', 'STANDARD', 16),
        ('2X-Large', 'STANDARD', 32),
        ('3X-Large', 'STANDARD', 64),
        ('4X-Large', 'STANDARD', 128),
        ('5X-Large', 'STANDARD', 256),
        ('6X-Large', 'STANDARD', 512),
        ('Medium', 'SNOWPARK-OPTIMIZED', 6),
        ('Large', 'SNOWPARK-OPTIMIZED', 12),
        ('X-Large', 'SNOWPARK-OPTIMIZED', 24),
        ('2X-Large', 'SNOWPARK-OPTIMIZED', 48),
        ('3X-Large', 'SNOWPARK-OPTIMIZED', 96),
        ('4X-Large', 'SNOWPARK-OPTIMIZED', 192),
        ('5X-Large', 'SNOWPARK-OPTIMIZED', 384),
        ('6X-Large', 'SNOWPARK-OPTIMIZED', 768)
    ) as t (warehouse_size, warehouse_type, credits_per_hour)
)

select
    warehouse_size,
    warehouse_type,
    credits_per_hour,
    credits_per_hour / 60 as credits_per_minute,
    credits_per_hour / 3600 as credits_per_second
from warehouse_credits_map
