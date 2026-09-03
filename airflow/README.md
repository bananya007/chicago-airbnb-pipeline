# Airflow snapshot schedule

`chicago_airbnb_snapshot_load` runs at 02:00 on the first day of January, April, July, and October (`0 2 1 1,4,7,10 *`).

Inside Airbnb publishes snapshots on irregular dates, so the DAG reads the current published date from the Airflow Variable `airbnb_snapshot_date`. Update that variable when a new quarterly snapshot is available:

```bash
docker compose -f airflow/docker-compose.yml exec airflow \
  airflow variables set airbnb_snapshot_date YYYY-MM-DD
```

Manual runs may override the variable with `{"snapshot_date":"YYYY-MM-DD"}` in the DAG run configuration. The default date remains `2026-06-24` until the next published snapshot is configured.
