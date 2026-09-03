"""Parameterized Chicago Airbnb snapshot load DAG."""

from __future__ import annotations

import csv
import gzip
import os
import re
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

import snowflake.connector
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sensors.python import PythonSensor

FILES = ("listings", "calendar", "reviews")
BASE_URL = "https://data.insideairbnb.com/united-states/il/chicago/{date}/data/{file}.csv.gz"
DATA_PAGE_URL = "https://insideairbnb.com/get-the-data/"
INITIAL_SNAPSHOT_DATE = "2026-06-24"


def resolve_snapshot_date(**context):
    override = context["dag_run"].conf.get("snapshot_date")
    if override:
        return override

    with urllib.request.urlopen(DATA_PAGE_URL, timeout=30) as response:
        page = response.read().decode("utf-8")
    chicago = re.search(
        r"<h3[^>]*>\s*Chicago[^<]*</h3>(.*?)(?=<h3\b|\Z)",
        page,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not chicago:
        raise AirflowException("Could not find Chicago on the Inside Airbnb data page")
    published = re.search(
        r"<h4[^>]*>\s*(\d{1,2} [A-Za-z]+, \d{4})", chicago.group(1)
    )
    if not published:
        raise AirflowException("Could not find the latest Chicago snapshot date")
    return datetime.strptime(published.group(1), "%d %B, %Y").date().isoformat()


def should_process_snapshot(**context):
    dag_run = context["dag_run"]
    latest = context["ti"].xcom_pull(task_ids="resolve_snapshot_date")
    last_loaded = Variable.get("airbnb_snapshot_date", default_var=INITIAL_SNAPSHOT_DATE)
    if dag_run.conf.get("snapshot_date"):
        print(f"Manual snapshot override requested: {latest}")
        return True
    if latest <= last_loaded:
        print(f"No new snapshot: latest={latest}, last_loaded={last_loaded}")
        return False
    print(f"New snapshot available: {latest} (last_loaded={last_loaded})")
    return True


def update_last_loaded_snapshot(**context):
    processed = context["ti"].xcom_pull(task_ids="resolve_snapshot_date")
    current = Variable.get("airbnb_snapshot_date", default_var=INITIAL_SNAPSHOT_DATE)
    if processed > current:
        Variable.set("airbnb_snapshot_date", processed)
        print(f"Updated airbnb_snapshot_date from {current} to {processed}")
    else:
        print(f"Kept airbnb_snapshot_date at {current}; processed={processed}")


def download_and_upload(**context):
    snapshot_date = context["ti"].xcom_pull(task_ids="resolve_snapshot_date")
    bucket = os.environ["AIRBNB_S3_BUCKET"]
    import boto3

    s3 = boto3.client("s3")
    local_dir = Path("/opt/airflow/project/data/raw") / snapshot_date
    local_dir.mkdir(parents=True, exist_ok=True)

    for file_name in FILES:
        local_path = local_dir / f"{file_name}.csv.gz"
        urllib.request.urlretrieve(
            BASE_URL.format(date=snapshot_date, file=file_name), local_path
        )
        with gzip.open(local_path, "rt", newline="", encoding="utf-8") as handle:
            next(csv.reader(handle))
        s3.upload_file(
            str(local_path),
            bucket,
            f"raw/{file_name}/dt={snapshot_date}/{file_name}.csv.gz",
        )


def snowpipe_loaded(**context):
    snapshot_date = context["ti"].xcom_pull(task_ids="resolve_snapshot_date")
    conn = snowflake.connector.connect(
        account=os.environ["AIRBNB_SNOWFLAKE_ACCOUNT"],
        user=os.environ["AIRBNB_SNOWFLAKE_USER"],
        password=os.environ["AIRBNB_SNOWFLAKE_PASSWORD"],
        role=os.environ.get("AIRBNB_SNOWFLAKE_ROLE", "DBT_ROLE"),
        warehouse=os.environ.get("AIRBNB_SNOWFLAKE_WAREHOUSE", "TRANSFORM_WH"),
        database=os.environ.get("AIRBNB_SNOWFLAKE_DATABASE", "AIRBNB"),
        schema="RAW",
    )
    try:
        cur = conn.cursor()
        for table, prefix in (
            ("CALENDAR", "calendar"),
            ("LISTINGS", "listings"),
            ("REVIEWS", "reviews"),
        ):
            # A rerun may reuse a filename already recorded by Snowpipe.
            # Check landed rows as well as recent COPY_HISTORY so valid
            # idempotent reruns do not wait for a new Snowpipe event.
            source_pattern = f"raw/{prefix}/dt={snapshot_date}/%"
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM AIRBNB.RAW.{table}
                WHERE _SOURCE_FILE ILIKE %s
                """,
                (source_pattern,),
            )
            landed_rows = cur.fetchone()[0]
            print(f"{table}: {landed_rows} landed rows for {source_pattern}")
            if landed_rows < 1:
                raise AirflowException(
                    f"{table} file for {snapshot_date} is not loaded by Snowpipe"
                )
        return True
    finally:
        conn.close()


with DAG(
    dag_id="chicago_airbnb_snapshot_load",
    start_date=datetime(2026, 1, 1),
    # Check weekly because source publication dates are not exact quarter dates.
    # The ETL path runs only when discovery finds a date newer than the watermark.
    schedule="0 2 * * 1",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "email": os.environ.get("AIRFLOW_ALERT_EMAIL"),
        "email_on_failure": True,
        "email_on_retry": False,
    },
    tags=["airbnb", "snowpipe", "dbt"],
) as dag:
    resolve_snapshot = PythonOperator(
        task_id="resolve_snapshot_date",
        python_callable=resolve_snapshot_date,
    )

    process_snapshot = ShortCircuitOperator(
        task_id="process_snapshot_if_new",
        python_callable=should_process_snapshot,
    )

    upload_snapshot = PythonOperator(
        task_id="download_and_upload_snapshot",
        python_callable=download_and_upload,
    )

    wait_for_snowpipe = PythonSensor(
        task_id="wait_for_snowpipe",
        python_callable=snowpipe_loaded,
        mode="reschedule",
        poke_interval=30,
        timeout=1800,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "dbt deps --project-dir /opt/airflow/project/dbt "
            "--log-path /tmp/dbt-logs "
            "--no-partial-parse "
            "&& dbt build --project-dir /opt/airflow/project/dbt "
            "--profiles-dir /opt/airflow/include --target prod "
            "--target-path /tmp/dbt-target --log-path /tmp/dbt-logs "
            "--no-partial-parse"
        ),
    )

    update_snapshot_pointer = PythonOperator(
        task_id="update_last_loaded_snapshot",
        python_callable=update_last_loaded_snapshot,
    )

    resolve_snapshot >> process_snapshot >> upload_snapshot
    upload_snapshot >> wait_for_snowpipe >> dbt_build >> update_snapshot_pointer
