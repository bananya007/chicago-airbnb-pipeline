# Chicago Airbnb Data Pipeline

An ELT pipeline for loading quarterly Chicago Airbnb listing, calendar, and review snapshots into Snowflake and publishing analytics-ready Kimball marts with dbt.

## Architecture

```text
Inside Airbnb snapshots → Airflow → Amazon S3 → Snowpipe → Snowflake RAW → dbt staging → dbt marts
```

### Components

- **Source:** Inside Airbnb Chicago CSV snapshots for listings, calendar, and reviews.
- **Object storage:** Amazon S3, organized as `raw/<entity>/dt=<snapshot_date>/`.
- **Ingestion:** Snowflake external stage and three Snowpipes using S3 event notifications.
- **Warehouse:** Snowflake database `AIRBNB`, with `RAW`, staging, marts, and audit schemas.
- **Transformation:** dbt models using staging views, SCD Type 2 dimensions, and fact tables.
- **Orchestration:** Apache Airflow running in Docker Compose.
- **Validation:** dbt tests plus GitHub Actions Python compilation and dbt parse checks.

## Data model

The warehouse retains every snapshot and derives history from the snapshot date.

- `stg_listings`: one row per listing and snapshot date.
- `stg_calendar`: one row per listing, calendar date, and snapshot date.
- `stg_reviews`: one row per listing and review.
- `dim_listing`: SCD Type 2 listing history.
- `dim_host`: SCD Type 2 host history.
- `dim_date`: reusable calendar dimension.
- `fact_calendar_day`: daily listing availability and pricing facts.
- `fct_reviews`: review facts.
- `est_occupancy`: scenario-based occupancy and revenue estimates.
- `pricing_gap`: current listing price compared with peer medians.

Surrogate keys are deterministic hashes of the model grain. This keeps keys stable across reruns while allowing multiple historical versions of the same listing or host.

## Reliability design

The pipeline is designed to be safely rerun after partial failures:

1. Snowflake load history prevents the same S3 file from being processed twice.
2. Staging models use stable grain keys and `row_number()`-based deduplication for repeated files under different names.
3. SCD2 models compare tracked attributes across snapshots and create non-overlapping validity windows.
4. Airflow retries failed tasks and waits for all three Snowpipes before starting dbt.
5. The sensor also accepts already-landed rows, so rerunning an idempotent load does not wait for a new event.

## Airflow schedule

The `chicago_airbnb_snapshot_load` DAG runs quarterly at 02:00 on January 1, April 1, July 1, and October 1. Each run discovers the latest Chicago publication date from Inside Airbnb and compares it with the Airflow Variable `airbnb_snapshot_date`, which represents the last successfully loaded snapshot.

If no newer snapshot exists, the DAG exits without processing. After a successful dbt build, the DAG updates the Variable automatically. A failed run leaves the pointer unchanged so it can be safely retried.

Manual runs can override the date with:

```json
{"snapshot_date":"YYYY-MM-DD"}
```

## Local setup

Create `.env` in the repository root with the Snowflake credentials and AWS configuration described in `airflow/.env.example`. The AWS CLI profile mounted into the container must have permission to upload to the configured S3 bucket.

Start Airflow:

```bash
docker compose -f airflow/docker-compose.yml up -d --build
```

Open `http://localhost:8080`, unpause `chicago_airbnb_snapshot_load`, and trigger it with an optional `snapshot_date` override.

## dbt commands

```bash
cd airbnb_pipeline
dbt deps
dbt build --profiles-dir ../airflow/include --target prod
```

The project currently contains 27 dbt tests covering uniqueness, nullability, relationships, SCD2 windows, positive prices, occupancy bounds, and orphan-row share.

## CI

GitHub Actions runs on pull requests and pushes to `main`. It compiles Python sources, installs the pinned dbt runtime, resolves packages, and parses the dbt project with credential placeholders. It does not connect to the production Snowflake account or run warehouse-changing models.

## Operations

- Monitor Snowpipe status and raw-table row counts when ingestion is delayed.
- Check Airflow task logs when the sensor times out or dbt fails.
- Rerun the DAG with the same `snapshot_date`; file-level and row-level idempotency make this safe.
- Keep credentials in local environment variables or a secret manager; never commit `.env`.

## Repository layout

```text
airflow/          Airflow image, DAG, Compose runtime, and dbt profile
airbnb_pipeline/  dbt project, staging models, marts, macros, and tests
ingestion/        Source download utilities
snowflake/        Warehouse, stage, pipe, and load-audit SQL
.github/          CI workflow
data/             Local-only downloaded snapshots
```
