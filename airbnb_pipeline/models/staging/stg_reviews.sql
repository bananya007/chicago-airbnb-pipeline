with source as (
    select * from {{ source('airbnb_raw', 'REVIEWS') }}
),
deduped as (
    select
        cast(id as number)                               as review_id,
        cast(listing_id as number)                       as listing_id,
        to_date(date)                                    as review_date,
        try_cast(reviewer_id as number)                  as reviewer_id,
        {{ extract_snapshot_date('_source_file') }}      as first_seen_snapshot,
        row_number() over (
            partition by cast(listing_id as number), cast(id as number)
            order by {{ extract_snapshot_date('_source_file') }}, _file_row
        ) as rn
    from source
)
select
    {{ dbt_utils.generate_surrogate_key(['listing_id', 'review_id']) }} as review_key,
    * exclude rn
from deduped
where rn = 1
