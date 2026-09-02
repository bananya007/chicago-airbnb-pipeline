{{ config(materialized='table') }}
with rates as (
    select column1 as review_rate
    from values (0.305), (0.50), (0.72)
),
monthly_reviews as (
    select listing_id,
           date_trunc('month', review_date) as month_start,
           count(*)                         as reviews_in_month
    from {{ ref('fct_reviews') }}
    group by 1, 2
),
calendar_ranked as (
    select *,
           row_number() over (
               partition by listing_id, date_trunc('month', calendar_date)
               order by snapshot_date desc
           ) as rn
    from {{ ref('fact_calendar_day') }}
),
monthly_calendar as (
    select listing_id,
           date_trunc('month', calendar_date) as month_start,
           avg(price)                          as avg_price,
           avg(minimum_nights)                 as avg_min_nights
    from calendar_ranked
    where rn = 1
    group by 1, 2
),
combined as (
    select r.listing_id, r.month_start, rates.review_rate, r.reviews_in_month,
           c.avg_price, c.avg_min_nights,
           day(last_day(r.month_start))                        as days_in_month,
           r.reviews_in_month / rates.review_rate              as est_bookings
    from monthly_reviews r
    cross join rates
    left join monthly_calendar c
      on r.listing_id = c.listing_id and r.month_start = c.month_start
)
select *,
    est_bookings * greatest(3, coalesce(avg_min_nights, 3))    as est_nights,
    least(est_bookings * greatest(3, coalesce(avg_min_nights, 3)) / days_in_month, 0.70)
                                                                as est_occupancy,
    least(est_bookings * greatest(3, coalesce(avg_min_nights, 3)) / days_in_month, 0.70)
        * days_in_month * coalesce(avg_price, 0)                as est_revenue
from combined
