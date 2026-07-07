{{ config(materialized='table') }}
with listings as (
    select * from {{ ref('stg_listings') }}
),
hashed as (
    select *,
        {{ dbt_utils.generate_surrogate_key([
            'neighbourhood',
            'room_type',
            'property_type',
            'accommodates',
            'bedrooms',
            'beds',
            'minimum_nights'
        ]) }} as attr_hash
    from listings 
),
with_prev as (  
    select *,
        lag(attr_hash) over (partition by listing_id order by snapshot_date) as prev_attr_hash
    from hashed
),
version_starts as ( 
    select * from with_prev
    where prev_attr_hash is null or attr_hash <> prev_attr_hash
),
versioned as (  
    select *,
        snapshot_date as valid_from,
        lead(snapshot_date) over (partition by listing_id order by snapshot_date) as valid_to
    from version_starts
)
select
    {{ dbt_utils.generate_surrogate_key(['listing_id', 'valid_from']) }} as listing_key,
    listing_id,
    listing_name,
    host_id,
    neighbourhood,
    room_type,
    property_type,
    accommodates,
    bedrooms,
    beds,
    minimum_nights,
    valid_from,
    valid_to,
    (valid_to is null) as is_current
from versioned

union all  

select
    '-1', null, 'Unknown', null, null, null, null,
    null, null, null, null,
    to_date('1900-01-01'), null, true
