create pipe airbnb.raw.pipe_calendar
    auto_ingest = TRUE
as
    copy into airbnb.raw.calendar(
        listing_id, date, available, price, adjusted_price, minimum_nights, maximum_nights, _source_file, _file_row
    )
    from (
        select $1, $2, $3, $4, $5, $6, $7, metadata$filename, metadata$file_row_number
        from @airbnb.raw.s3_raw/calendar/
    )
    file_format = (format_name = airbnb.raw.csv_gz);