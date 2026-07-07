create or replace pipe airbnb.raw.pipe_calendar
    auto_ingest = TRUE
as
    copy into airbnb.raw.calendar(
        listing_id, date, available, price, adjusted_price, minimum_nights, maximum_nights, _source_file, _file_row
    )
    from (
        select $1, $2, $3,
               NULL, NULL,      
               $4, $5,
               metadata$filename, metadata$file_row_number
        from @airbnb.raw.s3_raw/calendar/
    )
    file_format = (format_name = airbnb.raw.csv_gz)
    on_error = 'SKIP_FILE';
