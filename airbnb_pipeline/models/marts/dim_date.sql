{{ config(materialized='table') }}
with spine as (
    {{ dbt_utils.date_spine(depart='day',
        start_date="to_date('2010-01-01')"
        end_date="dateadd(day, 400, current_date)") }}
)

select 
    cast(date_day as date) as date_day,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month_num,
    to_cahr(date_day, 'MMMM') as month_name,
    dayofweek(date_day) as day_of _week,
    dayofweek(date_day) in (0, 6) as is_weekend
from spine