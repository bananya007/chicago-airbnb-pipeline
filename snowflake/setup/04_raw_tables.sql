CREATE TABLE AIRBNB.RAW.CALENDAR (
  listing_id       VARCHAR,
  date             VARCHAR,
  available        VARCHAR,
  price            VARCHAR,
  adjusted_price   VARCHAR,
  minimum_nights   VARCHAR,
  maximum_nights   VARCHAR,
  _source_file     VARCHAR,
  _file_row        NUMBER,
  _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE AIRBNB.RAW.REVIEWS (
  listing_id    VARCHAR,
  id            VARCHAR,
  date          VARCHAR,
  reviewer_id   VARCHAR,
  reviewer_name VARCHAR,
  comments      VARCHAR,
  _source_file  VARCHAR,
  _file_row     NUMBER,
  _loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);