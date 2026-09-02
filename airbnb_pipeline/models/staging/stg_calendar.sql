with source as (
    select * from {{ source('airbnb_raw', 'CALENDAR') }}
),
typed as (
    select
        cast(listing_id as number)                           as listing_id,
        to_date(date)                                        as calendar_date,
        (available = 't')                                    as is_available,
        try_cast(replace(replace(price,'$',''),',','') as number(10,2)) as price,
        try_cast(minimum_nights as number)                   as minimum_nights,
        try_cast(maximum_nights as number)                   as maximum_nights,
        {{ extract_snapshot_date('_source_file') }}          as snapshot_date,
        row_number() over (
            partition by cast(listing_id as number), to_date(date), {{ extract_snapshot_date('_source_file') }}
            order by _file_row
        ) as rn,
        _loaded_at
    from source
),
keyed as (
    select *,
           {{ dbt_utils.generate_surrogate_key(['listing_id', 'calendar_date', 'snapshot_date']) }} as calendar_day_key
    from typed
)
select * exclude rn
from keyed
where rn = 1
