{{ config(materialized='table') }}

select
    review_id,
    listing_id,
    review_date,
    reviewer_id,
    first_seen_snapshot
from {{ ref('stg_reviews') }}