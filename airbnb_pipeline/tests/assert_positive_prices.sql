{{ config(severity = 'warn') }}
select listing_id, price, snapshot_date
from {{ ref('stg_listings') }}
where price <= 0