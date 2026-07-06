create pipe airbnb.raw.pipe_reviews
    auto_ingest = TRUE
as 
    copy into airbnb.raw.reviews(
        listing_id, id, date, reviewer_id, reviewer_name, comments, _source_file, _file_row
    )
    from (
        select $1, $2, $3, $4, $5, $6, metadata$filename, metadata$file_row_number
        from @airbnb.raw.s3_raw/reviews/
    )
    file_format = (format_name = airbnb.raw.csv_gz);