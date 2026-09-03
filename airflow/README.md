# Airflow snapshot schedule

`chicago_airbnb_snapshot_load` runs at 02:00 on the first day of January, April, July, and October (`0 2 1 1,4,7,10 *`).

Inside Airbnb publishes snapshots on irregular dates, so the DAG discovers the current published date from the official data page. The Airflow Variable `airbnb_snapshot_date` stores the last successfully loaded snapshot; it is updated automatically only after the dbt build succeeds.

If the discovered date is not newer than the stored date, the DAG exits without downloading or transforming data. Manual runs may override discovery with `{"snapshot_date":"YYYY-MM-DD"}`; this runs the requested date but never moves the stored pointer backward.
