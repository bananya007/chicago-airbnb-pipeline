-- Corrective reload: 2026-06-24 calendar vintage (5 cols) into the 7-col table.
-- WHY: the 2026 vintage DROPPED price and adjusted_price from calendar
-- (header: listing_id, date, available, minimum_nights, maximum_nights).
-- The manual first load used the old 7-column positional mapping, so
-- minimum_nights landed in price and maximum_nights in adjusted_price.
-- Note: calendar price is effectively retired upstream anyway — 2025's price
-- column exists but is 0% populated; pricing now lives in the listings file.
--
-- RUN ORDER: delete shifted rows -> corrective COPY (FORCE) -> verify.

-- 1) remove the shifted 2026 rows
DELETE FROM AIRBNB.RAW.CALENDAR WHERE _source_file LIKE '%dt=2026-06-24%';

-- 2) corrective load, mapped by name against the 2026 header
COPY INTO AIRBNB.RAW.CALENDAR (
  listing_id, date, available, price, adjusted_price,
  minimum_nights, maximum_nights, _source_file, _file_row, _loaded_at
)
FROM (
  SELECT $1, $2, $3,
         NULL, NULL,          -- price, adjusted_price: not in 2026 vintage
         $4, $5,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
  FROM @AIRBNB.RAW.S3_RAW/calendar/dt=2026-06-24/
)
FILE_FORMAT = (FORMAT_NAME = AIRBNB.RAW.CSV_GZ)
ON_ERROR = 'ABORT_STATEMENT'
FORCE = TRUE;

-- 3) verify: min/max nights populated, price NULL for 2026; spot-check values are sane
SELECT
  CASE WHEN _source_file LIKE '%2026-06-24%' THEN '2026' ELSE '2025' END AS vintage,
  COUNT(*)                                   AS rows,
  COUNT(minimum_nights)                      AS min_nights_filled,
  COUNT(price)                               AS price_filled
FROM AIRBNB.RAW.CALENDAR
GROUP BY 1;
