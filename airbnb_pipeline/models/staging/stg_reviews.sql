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
            partition by cast(id as number)
            order by {{ extract_snapshot_date('_source_file') }}
        ) as rn
    from source
)
select * exclude rn from deduped where rn = 1