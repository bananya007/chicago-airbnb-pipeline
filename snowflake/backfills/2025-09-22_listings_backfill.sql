-- Corrective backfill: 2025-09-22 listings vintage (79 cols) into the 2026-shaped table (90 cols).
-- WHY: the 2026-06-24 vintage added 11 columns (host_profile_*, hosts_time_*, price_quote_*).
-- The pipe's positional mapping was generated from the 2026 header, so 2025 rows loaded shifted
-- (e.g. amenities landed in room_type). This COPY maps the 2025 header BY NAME; absent columns -> NULL.
--
-- RUN ORDER:
-- 1) DELETE the shifted rows
-- 2) COPY with FORCE = TRUE (file is already in load history from the bad load; safe only because of step 1)
-- 3) Verify

-- 1) remove the shifted rows
DELETE FROM AIRBNB.RAW.LISTINGS WHERE _source_file LIKE '%dt=2025-09-22%';

-- 2) corrective load, mapped by name against the 2025 header
COPY INTO AIRBNB.RAW.LISTINGS (
  id, listing_url, scrape_id, last_scraped, source, name, description,
  neighborhood_overview, picture_url, host_id, host_url,
  host_profile_id, host_profile_url,
  host_name, host_since,
  hosts_time_as_user_years, hosts_time_as_user_months,
  hosts_time_as_host_years, hosts_time_as_host_months,
  host_location, host_about, host_response_time, host_response_rate,
  host_acceptance_rate, host_is_superhost, host_thumbnail_url,
  host_picture_url, host_neighbourhood, host_listings_count,
  host_total_listings_count, host_verifications, host_has_profile_pic,
  host_identity_verified, neighbourhood, neighbourhood_cleansed,
  neighbourhood_group_cleansed, latitude, longitude, property_type,
  room_type, accommodates, bathrooms, bathrooms_text, bedrooms, beds,
  amenities, price,
  price_quote_checkin_date, price_quote_checkout_date,
  price_quote_total_price, price_quote_price_per_night, price_quote_raw,
  minimum_nights, maximum_nights, minimum_minimum_nights,
  maximum_minimum_nights, minimum_maximum_nights, maximum_maximum_nights,
  minimum_nights_avg_ntm, maximum_nights_avg_ntm, calendar_updated,
  has_availability, availability_30, availability_60, availability_90,
  availability_365, calendar_last_scraped, number_of_reviews,
  number_of_reviews_ltm, number_of_reviews_l30d, availability_eoy,
  number_of_reviews_ly, estimated_occupancy_l365d, estimated_revenue_l365d,
  first_review, last_review, review_scores_rating, review_scores_accuracy,
  review_scores_cleanliness, review_scores_checkin,
  review_scores_communication, review_scores_location,
  review_scores_value, license, instant_bookable,
  calculated_host_listings_count, calculated_host_listings_count_entire_homes,
  calculated_host_listings_count_private_rooms,
  calculated_host_listings_count_shared_rooms, reviews_per_month,
  _source_file, _file_row, _loaded_at
)
FROM (
  SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
         NULL, NULL,                       -- host_profile_id/url: not in 2025 vintage
         $12, $13,
         NULL, NULL, NULL, NULL,           -- hosts_time_*: not in 2025 vintage
         $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26,
         $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39,
         $40, $41,
         NULL, NULL, NULL, NULL, NULL,     -- price_quote_*: not in 2025 vintage
         $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54,
         $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65, $66, $67,
         $68, $69, $70, $71, $72, $73, $74, $75, $76, $77, $78, $79,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
  FROM @AIRBNB.RAW.S3_RAW/listings/dt=2025-09-22/
)
FILE_FORMAT = (FORMAT_NAME = AIRBNB.RAW.CSV_GZ)
ON_ERROR = 'ABORT_STATEMENT'
FORCE = TRUE;

-- 3) verify: room_type should now contain only the 4 valid values for the 2025 rows
SELECT room_type, COUNT(*)
FROM AIRBNB.RAW.LISTINGS
WHERE _source_file LIKE '%dt=2025-09-22%'
GROUP BY 1 ORDER BY 2 DESC;
