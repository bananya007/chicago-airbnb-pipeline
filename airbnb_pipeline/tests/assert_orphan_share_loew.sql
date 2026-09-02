{{ config(severity = 'warn') }}
select
    count_if(listing_key = '-1') as orphan_rows,
    count(*)                     as total_rows,
    orphan_rows / total_rows     as orphan_share
from {{ ref('fact_calendar_day') }}
having orphan_share > 0.01
