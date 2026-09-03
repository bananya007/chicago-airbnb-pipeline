"""Parameterized Chicago Airbnb snapshot load DAG."""

from __future__ import annotations

import csv
import gzip
import os
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

import snowflake.connector
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from airflow.models import Variable

FILES = ("listings", "calendar", "reviews")
BASE_URL = "https://data.insideairbnb.com/united-states/il/chicago/{date}/data/{file}.csv.gz"
DEFAULT_SNAPSHOT_DATE = Variable.get(
    "airbnb_snapshot_date", default_var="2026-06-24"
)


def download_and_upload(**context):
    snapshot_date = context["dag_run"].conf.get(
        "snapshot_date", context["params"]["snapshot_date"]
    )
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
    snapshot_date = context["dag_run"].conf.get(
        "snapshot_date", context["params"]["snapshot_date"]
    )
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
    schedule="0 2 1 1,4,7,10 *",
    catchup=False,
    max_active_runs=1,
    params={"snapshot_date": DEFAULT_SNAPSHOT_DATE},
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "email_on_failure": True,
        "email_on_retry": False,
    },
    tags=["airbnb", "snowpipe", "dbt"],
) as dag:
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

    upload_snapshot >> wait_for_snowpipe >> dbt_build
