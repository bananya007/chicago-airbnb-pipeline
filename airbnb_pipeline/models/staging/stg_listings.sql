-- dbt/models/staging/stg_listings.sql
with source as (
    select * from {{ source('airbnb_raw', 'LISTINGS') }}
)
select
    cast(id as number)                                   as listing_id,
    name                                                 as listing_name,
    neighbourhood_cleansed                               as neighbourhood,
    room_type,
    property_type,
    try_cast(accommodates as number)                     as accommodates,
    try_cast(bedrooms as number)                         as bedrooms,
    try_cast(beds as number)                             as beds,
    try_cast(replace(replace(price,'$',''),',','') as number(10,2)) as price,
    try_cast(minimum_nights as number)                   as minimum_nights,
    try_cast(availability_365 as number)                 as availability_365,
    try_cast(number_of_reviews as number)                as number_of_reviews,
    try_cast(review_scores_rating as number(3,2))        as review_scores_rating,
    cast(host_id as number)                              as host_id,
    {{ extract_snapshot_date('_source_file') }}          as snapshot_date
from source