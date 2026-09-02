{{ config(materialized='table') }}
with latest as (
    select *
    from {{ ref('stg_listings') }}
    where snapshot_date = (select max(snapshot_date) from {{ ref('stg_listings') }})
      and price is not null
),
peers as (
    select
        neighbourhood,
        room_type,
        median(price) as peer_median_price,
        count(*)      as peer_count
    from latest
    group by 1, 2
)
select
    l.listing_id,
    dl.listing_key,
    l.neighbourhood,
    l.room_type,
    l.price                                            as current_price,
    p.peer_median_price,
    p.peer_count,
    case when p.peer_count >= 5
         then round((l.price - p.peer_median_price) / nullif(p.peer_median_price, 0), 4)
    end                                                as price_gap_pct
from latest l
join peers p
  using (neighbourhood, room_type)
left join {{ ref('dim_listing') }} dl
       on l.listing_id = dl.listing_id and dl.is_current
