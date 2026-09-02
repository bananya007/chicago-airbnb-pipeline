with dims as (
    select 'dim_host' as dim, host_id as nk, valid_from, valid_to, is_current
    from {{ ref('dim_host') }} where host_id is not null
    union all
    select 'dim_listing', listing_id, valid_from, valid_to, is_current
    from {{ ref('dim_listing') }} where listing_id is not null
),
window_check as (
    select *,
        lead(valid_from) over (partition by dim, nk order by valid_from) as next_valid_from
    from dims
),
bad_windows as (
    select dim, nk, 'window overlap' as problem
    from window_check
    where next_valid_from is not null
      and valid_to > next_valid_from
),
bad_current as (
    select dim, nk, 'current-row count <> 1' as problem
    from dims
    group by dim, nk
    having count_if(is_current) <> 1
)
select * from bad_windows
union all
select * from bad_current
