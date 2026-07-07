with source as (
    select * from {{ source('airbnb_raw', 'LISTINGS') }}
),
extracted as (
    select
        cast(host_id as number)                          as host_id,
        host_name,
        to_date(host_since)                              as host_since,
        host_response_time,
        try_cast(replace(host_response_rate,'%','') as number)/100   as host_response_rate,
        try_cast(replace(host_acceptance_rate,'%','') as number)/100 as host_acceptance_rate,
        (host_is_superhost = 't')                        as is_superhost,
        try_cast(host_listings_count as number)          as host_listings_count,
        (host_identity_verified = 't')                   as is_identity_verified,
        {{ extract_snapshot_date('_source_file') }}      as snapshot_date,
        row_number() over (
            partition by host_id, {{ extract_snapshot_date('_source_file') }}
            order by _file_row
        ) as rn
    from source
    where host_id is not null
)
select * exclude rn from extracted where rn = 1