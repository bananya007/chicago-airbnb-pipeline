{{ config(materialized='table') }}
with hosts as(
    select * from {{ ref('stg_hosts') }}
),
hashed as (
    select *,
        {{ dbt_utils.generate_surrogate_key([
            'is_superhost','host_response_time','host_response_rate','host_acceptance_rate','host_listings_count',
            'is_identity_verified'
        ]) }} as attr_hash
        from hosts
),
with_prev as (
    select *,
        lag(attr_hash) over (partition by host_id order by snapshot_date) as prev_attr_hash
        from hashed 
),
version_starts as (
    select *
    from with_prev
    where prev_attr_hash is null or attr_hash <> prev_attr_hash
),
versioned as (
    select *,
        snapshot_date as valid_from,
        lead(snapshot_date) over (partition by host_id order by snapshot_date) as valid_to
        from version_starts
)
select 
    {{ dbt_utils.generate_surrogate_key(['host_id','valid_from']) }} as host_key,
    host_id,
    host_name,
    host_since,
    host_response_time,
    host_response_rate,
    host_acceptance_rate,
    is_superhost,
    host_listings_count,
    is_identity_verified,
    valid_from,
    valid_to,
    (valid_to is null) as is_current
from versioned

union all 

select '-1', null, 'Unknown', null, null, null, null, null, null, null, to_date('1900-01-01'), null, TRUE
