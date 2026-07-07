with source as (
    select * from {{ source('airbnb_raw', 'CALENDAR') }}
)
select
    cast(listing_id as number)                           as listing_id,
    to_date(date)                                        as calendar_date,
    (available = 't')                                    as is_available,
    try_cast(replace(replace(price,'$',''),',','') as number(10,2)) as price,
    try_cast(minimum_nights as number)                   as minimum_nights,
    try_cast(maximum_nights as number)                   as maximum_nights,
    {{ extract_snapshot_date('_source_file') }}          as snapshot_date,
    _loaded_at
from source