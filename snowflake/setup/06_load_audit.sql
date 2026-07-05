CREATE VIEW AIRBNB.AUDIT.LOAD_HISTORY AS
SELECT file_name, table_name, status,
       row_count, row_parsed, first_error_message, last_load_time
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
       TABLE_NAME => 'AIRBNB.RAW.CALENDAR',
       START_TIME => DATEADD(day, -14, CURRENT_TIMESTAMP())))
UNION ALL
SELECT file_name, table_name, status,
       row_count, row_parsed, first_error_message, last_load_time
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
       TABLE_NAME => 'AIRBNB.RAW.LISTINGS',
       START_TIME => DATEADD(day, -14, CURRENT_TIMESTAMP())))
UNION ALL
SELECT file_name, table_name, status,
       row_count, row_parsed, first_error_message, last_load_time
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
       TABLE_NAME => 'AIRBNB.RAW.REVIEWS',
       START_TIME => DATEADD(day, -14, CURRENT_TIMESTAMP())));