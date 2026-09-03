# Airflow snapshot schedule

`chicago_airbnb_snapshot_load` checks for a new source publication every Monday at 02:00 (`0 2 * * 1`). The ETL path runs only when the discovered date is newer than the stored watermark, so the pipeline still processes quarterly snapshots rather than reloading every week.

Inside Airbnb publishes snapshots on irregular dates, so the DAG discovers the current published date from the official data page. The Airflow Variable `airbnb_snapshot_date` stores the last successfully loaded snapshot; it is updated automatically only after the dbt build succeeds.

If the discovered date is not newer than the stored date, the DAG exits without downloading or transforming data. Manual runs may override discovery with `{"snapshot_date":"YYYY-MM-DD"}`; this runs the requested date but never moves the stored pointer backward.
