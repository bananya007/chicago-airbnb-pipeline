{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'calendar_day_key',
    cluster_by = ['calendar_date']
)}}
with cal as (
    select * from {{ ref('stg_calendar') }}
    {% if is_incremental() %} 
        where snapshot_date >= (select coalesce(max(snapshot_date), to_date('1900-01-01')) from {{ this }})
    {% endif %}
),
listing_link as (
    select listing_id, host_id, snapshot_date 
    from {{ ref('stg_listings') }}
)
select 
    {{ dbt_utils.generate_surrogate_key(['c.listing_id','c.calendar_date','c.snapshot_date']) }} as calendar_day_key,
    coalesce(dl.listing_key, '-1') as listing_key,
    coalesce(dh.host_key, '-1') as host_key,
    c.listing_id,
    c.calendar_date,
    c.is_available,
    c.price,
    c.minimum_nights,
    c.snapshot_date
from cal c
left join listing_link ll 
    on c.listing_id = ll.listing_id and c.snapshot_date = ll.snapshot_date 
left join {{ ref('dim_listing') }} dl 
    on c.listing_id = dl.listing_id 
    and c.snapshot_date >= dl.valid_from 
    and (c.snapshot_date < dl.valid_to or dl.valid_to is null)
left join {{ ref('dim_host') }} dh 
    on ll.host_id = dh.host_id 
    and c.snapshot_date >= dh.valid_from
    and (c.snapshot_date < dh.valid_to or dh.valid_to is null)