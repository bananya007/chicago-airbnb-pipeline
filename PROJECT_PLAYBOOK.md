# Chicago Airbnb Pipeline — Build Playbook

**Goal:** A production-grade, end-to-end ELT pipeline using real Inside Airbnb data:
S3 → Snowpipe → Snowflake → dbt (staging → marts, SCD2, tested) → orchestrated by Airflow → monitored with Elementary → CI/CD via GitHub Actions → Tableau dashboards.

**Why this project exists (your interview one-liner):** "I rebuilt my portfolio pipeline around real quarterly-refreshed data and production patterns — idempotent ingestion, failure recovery, monitoring, alerting, and CI — so every claim on my resume is something I can demo and defend."

**How to use this doc:**
- This is a **living document**. All phases (0–6) are now written in full execution detail, each finalized after discussion. If building a phase changes your understanding, update the later phases — that's the point of the format.
- Check off boxes as you go. If a step fails, note what happened next to it — those notes become interview stories.
- Never skip the ⚠️ **Break it on purpose** exercises. They are the whole point.

**Rule for your resume:** a bullet is only added after the phase is done, broken once on purpose, and you can explain it for 10 minutes.

## How you'll work: the branch → PR → merge loop (used in every phase)

Every phase produces at least one Pull Request. Never commit to `main` directly — the PR history *is* part of the portfolio. The loop:

```bash
# 1. start clean, on main, up to date
git checkout main && git pull

# 2. branch for the phase (names are given in each phase)
git checkout -b feature/<phase-name>

# 3. work in small commits — one logical change each, message says WHY
git add -A && git commit -m "add snowpipe for calendar: auto-ingest on S3 event"

# 4. push the branch up
git push -u origin feature/<phase-name>
```

Then on github.com: a yellow **"Compare & pull request"** banner appears → click it → write a real description (what changed, why, how you verified it) → **review your own diff** in the Files tab (you'll catch something, always) → **Squash and merge** → delete the branch. Back home: `git checkout main && git pull`.

**When to raise the PR:** when the checklist items you set out to do in that sitting pass their checks — not necessarily a whole phase; big phases (1, 3) are better as 2–3 smaller PRs (e.g., `feature/ingestion-s3`, then `feature/ingestion-snowpipe`). Small PRs are easier to review and tell a better story.
**When to merge:** after you've reviewed your own diff — and from Phase 6 onward, only after CI checks are green (GitHub will block you; that's the point).

---

## Architecture (target state)

```
Inside Airbnb quarterly snapshot (CSV.gz)
        │  (Python download script)
        ▼
S3  raw/<entity>/dt=<snapshot_date>/file.csv.gz
        │  (S3 event → SQS → Snowpipe auto-ingest)
        ▼
Snowflake RAW schema (typed-as-text landing tables + load audit)
        │  (dbt, orchestrated by Airflow + Cosmos)
        ▼
STAGING schema (cleaning, typing, host/listing split)
        ▼
MARTS schema (fct_calendar_day, dim_listing, dim_host SCD2, est. occupancy)
        ▼
Tableau dashboards          Airflow monitoring + email alerts
```

## Problem statement — what this pipeline is *for*

**The business framing (your "so what?" answer):** A Chicago Airbnb host — or a property manager running several listings — has no data team. They set prices by gut feel. This pipeline turns Inside Airbnb's public quarterly snapshots into pricing and occupancy intelligence they could actually act on.

**Questions the final dashboards must answer** (design the marts backward from these):

*Pricing*
1. How do listed nightly prices vary by month, neighborhood, and room type over the next 365 days?
2. Where are the pricing gaps — which listings are priced significantly below/above their neighborhood + room-type median?
3. Do minimum-night rules correlate with price positioning?

*Demand & occupancy* (estimated — reviews-based proxy, per Inside Airbnb's methodology)
4. Which neighborhoods and property types have the highest estimated occupancy?
5. Which months are booked out furthest in advance (lowest forward availability)?

*Revenue potential*
6. What is estimated revenue per listing (est. occupied nights × price), and which segments (neighborhood × room type) lead?

*Hosts*
7. Do superhosts and fast responders actually command higher prices / occupancy?
8. How did host quality change between snapshots — who gained/lost superhost status? (needs SCD2)

*Market movement* (needs 2+ snapshots)
9. Quarter over quarter: how many listings entered/left the market, and how did median prices move by neighborhood?

**Honesty caveat to state on the dashboard and in interviews:** prices are *listed* prices, not transactions; occupancy is *estimated* from review activity. Knowing exactly where the data's limits are is part of the credibility.

## Final deliverables (what "done" looks like)

1. **GitHub repo** `chicago-airbnb-pipeline` — ingestion code, Snowflake SQL, dbt project, Airflow DAG, CI workflows, all merged via PRs.
2. **A pipeline that runs without you** — Snowpipe ingestion + scheduled Airflow runs, with retries and alerts.
3. **Two Tableau Public dashboards** answering the questions above (Pricing & Market, Host & Occupancy).
4. **`docs/RUNBOOK.md`** — failure modes, recovery steps, backfill procedure.
5. **dbt docs site** (GitHub Pages) with full lineage source → dashboard.
6. **README** with architecture diagram, dashboard screenshots, the problem statement above, and data attribution.
7. **Two defensible resume bullets** — written last, from what was actually built.

**Phase map:**

| Phase | What | Status |
|---|---|---|
| 0 | Accounts, tools, new repo | ✅ Done (Jul 2026) |
| 1 | Real data → S3 → Snowflake ingestion (Snowpipe) | 🔨 In progress |
| 2 | dbt project + staging layer | Detailed below |
| 3 | Marts, SCD2 dims, estimated occupancy | Detailed below |
| 4 | Orchestration: Airflow + Cosmos | Detailed below |
| 5 | Reliability: email alerting, runbook, recovery drill | Detailed below |
| 6 | CI/CD, dbt docs, exposures, Tableau dashboards | Detailed below |

**Cost guardrails (whole project):** AWS free tier covers S3 at this size (<1 GB). Snowflake 30-day trial ($400 credits) covers everything — an XS warehouse with 60-second auto-suspend will barely dent it. Everything Snowflake-side lives as SQL scripts in the repo, so if the trial expires you can recreate the entire account setup in ~30 minutes (that reproducibility is itself an interview point).

---

# Phase 0 — Accounts, tools, and the new repo

**Outcome:** AWS + Snowflake accounts exist with billing safety rails; local machine has AWS CLI, Python venv, git; new GitHub repo `chicago-airbnb-pipeline` exists with the folder skeleton.

## 0.1 AWS account

- [ ] Go to https://aws.amazon.com → **Create an AWS Account**. Use a personal email. This creates the **root user**.
- [ ] You'll need a credit card. You will stay in free tier, but set the safety rail below anyway.
- [ ] Sign in to the AWS Console (https://console.aws.amazon.com), region selector (top right): choose **us-east-1 (N. Virginia)**. Write this down — **Snowflake must be created in the same region** (Step 0.2) or Snowpipe setup gets harder and cross-region transfer costs money.
- [ ] **Enable MFA on root:** Console → search "IAM" → Dashboard → "Add MFA for root user". Use any authenticator app.
- [ ] **Billing alarm:** Console → search "Budgets" → Create budget → template "Zero spend budget" → your email. You'll get an email if you spend one cent.
- [ ] **Create an admin IAM user** (never use root day-to-day):
  - IAM → Users → Create user → name: `ananya-admin` → check "Provide user access to the AWS Management Console" → set a password.
  - Permissions: "Attach policies directly" → `AdministratorAccess`.
  - *Real-world note for interviews: production accounts use least-privilege roles, not admin users. Fine for a solo sandbox; know the difference.*
- [ ] **Create access keys for the CLI:** IAM → Users → `ananya-admin` → Security credentials → Create access key → use case "Command Line Interface" → download the CSV. **Never commit these to git.**

## 0.2 Snowflake account

- [ ] Go to https://signup.snowflake.com → Start for free (30 days, $400 credits).
- [ ] Edition: **Enterprise** (trial default is fine; Standard also works). Cloud: **AWS**. Region: **US East (N. Virginia) / us-east-1** — must match your S3 region.
- [ ] After email verification you get an **account URL** like `https://<org>-<account>.snowflakecomputing.com`. Save it, plus username/password, somewhere safe (not in the repo).
- [ ] Log in to Snowsight (the web UI). Your user has the `ACCOUNTADMIN` role — you'll use it for setup scripts.
- [ ] Find your **account identifier**: Snowsight → click your name (bottom left) → Account → copy the "Account identifier" (format `ORGNAME-ACCOUNTNAME`). dbt needs this later.
- [ ] **Trial-expiry insurance:** everything we create in Snowflake is scripted in `snowflake/` in the repo. If the trial dies, sign up again and re-run the scripts. Nothing lives only in the UI.

## 0.3 Local tools (macOS)

- [ ] **Homebrew** (if missing): check with `brew --version`; install from https://brew.sh if needed.
- [ ] **git**: `git --version` (comes with Xcode CLT; accept the prompt if it appears).
- [ ] **AWS CLI:** `brew install awscli`, then `aws configure` — paste the access key + secret from 0.1, region `us-east-1`, output `json`. Verify: `aws sts get-caller-identity` should print your account ID.
- [ ] **Python 3.11+:** `python3 --version`. If <3.11: `brew install python@3.12`.
- [ ] **Docker Desktop** (needed in Phase 4 for Airflow; install now so it's ready): https://www.docker.com/products/docker-desktop → install → open once so it finishes setup.
- [ ] **VS Code** (or your editor of choice) with the "dbt Power User" extension — optional but helpful from Phase 2 on.

## 0.4 New GitHub repo

- [ ] On github.com → New repository → name: **`chicago-airbnb-pipeline`** → Public → add a README → add `.gitignore` template: `Python`.
- [ ] Clone it: `git clone https://github.com/bananya007/chicago-airbnb-pipeline.git`
- [ ] Create the skeleton (run inside the repo folder):

```bash
mkdir -p ingestion snowflake/setup snowflake/pipes data/raw docs
touch ingestion/.gitkeep snowflake/setup/.gitkeep snowflake/pipes/.gitkeep docs/.gitkeep
```

- [ ] Append to `.gitignore` (raw data and secrets never enter git):

```
# data files
data/
*.csv
*.csv.gz

# secrets
.env
*.pem
```

- [ ] **Branch discipline from day 1:** work happens on feature branches, merged to `main` via Pull Requests — even solo. Your commit/PR history is part of the portfolio. First PR: this skeleton.

```bash
git checkout -b feature/repo-skeleton
git add -A && git commit -m "repo skeleton: ingestion, snowflake, docs folders"
git push -u origin feature/repo-skeleton
# then open a PR on github.com and merge it
```

- [ ] Add to `README.md`: one paragraph on what the project will be, the architecture diagram from this doc, and this attribution line (the data license, CC BY 4.0, requires it):
  > Data sourced from [Inside Airbnb](https://insideairbnb.com), licensed under CC BY 4.0.

**✅ Phase 0 acceptance criteria**
- `aws sts get-caller-identity` works.
- You can log into Snowsight and run `SELECT CURRENT_ACCOUNT();`.
- Repo exists with skeleton, merged via a PR.
- Zero-spend budget alert is active.

---

# Phase 1 — Real data → S3 → Snowflake ingestion

**Outcome:** Inside Airbnb Chicago data lands in S3 under dated paths, Snowflake loads it via `COPY INTO` (then automatically via Snowpipe), loads are provably idempotent, and a load-audit view tells you what loaded when.

Every SQL file below is committed to the repo — that's the "ingestion as code" story. This phase is **3 PRs**; commit and PR steps appear inline below at the point you should do them. General rule: commit at every "it works" moment, push whenever you leave your desk.

## 1.1 Get the data — `ingestion/download_snapshot.py`

Inside Airbnb publishes quarterly snapshots per city. As of July 2026, Chicago has **2026-06-24** (current) and **2025-09-22** (previous) available — download both; two vintages make SCD2 and market-churn real. Always check https://insideairbnb.com/get-the-data/ for what's live. Note the page lists both detailed files (`.csv.gz`, under `/data/` — what this pipeline uses) and summary files (plain `.csv`, for quick visualizations) with the same base names — use the `.gz` ones.

- [ ] Create `ingestion/download_snapshot.py`:

```python
"""Download an Inside Airbnb snapshot for Chicago.
Usage: python ingestion/download_snapshot.py 2025-09-22
"""
import sys, urllib.request
from pathlib import Path

BASE = "https://data.insideairbnb.com/united-states/il/chicago/{date}/data/{file}"
FILES = ["listings.csv.gz", "calendar.csv.gz", "reviews.csv.gz"]

def main(snapshot_date: str):
    out_dir = Path("data/raw") / snapshot_date
    out_dir.mkdir(parents=True, exist_ok=True)
    for f in FILES:
        url = BASE.format(date=snapshot_date, file=f)
        dest = out_dir / f
        print(f"downloading {url}")
        urllib.request.urlretrieve(url, dest)
        print(f"  -> {dest} ({dest.stat().st_size:,} bytes)")

if __name__ == "__main__":
    main(sys.argv[1])
```

- [ ] Run it for both available snapshots:

```bash
python3 ingestion/download_snapshot.py 2026-06-24
python3 ingestion/download_snapshot.py 2025-09-22
```
- [ ] Sanity-check what you downloaded (columns matter for 1.4):

```bash
gzcat data/raw/2026-06-24/calendar.csv.gz | head -3
gzcat data/raw/2026-06-24/listings.csv.gz | head -1   # header only; ~75 columns
gzcat data/raw/2026-06-24/calendar.csv.gz | wc -l      # expect ~3 million
```

📝 *What you just learned: real data arrives compressed, wide, and messy — prices like "$1,250.00", free-text columns containing commas, quotes, and newlines. Keep these observations; they justify your staging layer in interviews.*

- [ ] 🔀 **Commit + PR #1** on branch `feature/ingestion-download`: the script, the `.gitignore` data entries, the README attribution. Review your own diff, merge, back to main.

## 1.2 S3 bucket + dated upload convention

- [ ] Create the bucket (names are globally unique — adjust the suffix):

```bash
aws s3 mb s3://chicago-airbnb-raw-ananya --region us-east-1
```

- [ ] Leave "Block all public access" ON (the default). Snowflake will get access via an IAM role, never via a public bucket.
- [ ] Upload with the **dated key convention** — this is a design decision, not housekeeping. `dt=<snapshot_date>` means every load is identifiable, re-runnable, and backfillable:

```bash
SNAP=2026-06-24   # first snapshot loads now; the second (2025-09-22) is saved for the Snowpipe test in 1.9
for f in listings calendar reviews; do
  aws s3 cp data/raw/$SNAP/$f.csv.gz \
    s3://chicago-airbnb-raw-ananya/raw/$f/dt=$SNAP/$f.csv.gz
done
```

- [ ] Verify: `aws s3 ls s3://chicago-airbnb-raw-ananya/raw/ --recursive`

## 1.3 Snowflake foundations — `snowflake/setup/01_database_warehouse.sql`

Start branch `feature/ingestion-snowflake` here — it carries you through 1.8. Run each script in Snowsight (paste into a worksheet, role `ACCOUNTADMIN`) **and** commit it to the repo, one commit per verified script.

```sql
-- 01_database_warehouse.sql
CREATE DATABASE IF NOT EXISTS AIRBNB;
CREATE SCHEMA IF NOT EXISTS AIRBNB.RAW;      -- landing zone, loaded by Snowpipe
CREATE SCHEMA IF NOT EXISTS AIRBNB.AUDIT;    -- load bookkeeping

CREATE WAREHOUSE IF NOT EXISTS TRANSFORM_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60          -- suspends after 60s idle: your credit guardrail
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;
```

## 1.4 Storage integration — `snowflake/setup/02_storage_integration.sql`

**Concept first:** a storage integration lets Snowflake read your bucket by *assuming an IAM role in your AWS account* — no AWS keys are ever stored in Snowflake. The setup is a handshake: create the integration pointing at a role name you choose → Snowflake tells you *its* AWS identity → you create the role trusting exactly that identity. (This is the governance answer to "how does Snowflake authenticate to S3?")

- [ ] Get your AWS account ID: `aws sts get-caller-identity --query Account --output text` (12 digits — used below as `<ACCOUNT_ID>`).
- [ ] In Snowsight, run (the role doesn't exist yet — that's expected):

```sql
-- 02_storage_integration.sql
CREATE STORAGE INTEGRATION s3_airbnb_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<ACCOUNT_ID>:role/snowflake-airbnb-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://chicago-airbnb-raw-ananya/raw/');

DESC INTEGRATION s3_airbnb_int;
```

- [ ] From the `DESC` output, copy two values: **STORAGE_AWS_IAM_USER_ARN** (Snowflake's AWS identity) and **STORAGE_AWS_EXTERNAL_ID**.
- [ ] AWS Console → IAM → **Policies** → Create policy → JSON tab:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:GetObjectVersion"],
      "Resource": "arn:aws:s3:::chicago-airbnb-raw-ananya/raw/*" },
    { "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::chicago-airbnb-raw-ananya" }
  ]
}
```

  Name it `snowflake-airbnb-s3-read`. *(Read-only: Snowflake never needs to write here.)*
- [ ] IAM → **Roles** → Create role → "AWS account" → "Another AWS account" → paste the **account ID portion** of STORAGE_AWS_IAM_USER_ARN → check **"Require external ID"** → paste STORAGE_AWS_EXTERNAL_ID → attach policy `snowflake-airbnb-s3-read` → name it exactly **`snowflake-airbnb-role`**.
- [ ] Back in Snowsight, verify the handshake works in 1.5.

## 1.5 File format + stage — `snowflake/setup/03_file_format_stage.sql`

```sql
-- 03_file_format_stage.sql
CREATE FILE FORMAT AIRBNB.RAW.CSV_GZ
  TYPE = 'CSV'
  COMPRESSION = 'GZIP'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'   -- critical: free-text fields contain commas AND newlines
  SKIP_HEADER = 1
  NULL_IF = ('', 'N/A', 'NULL')
  EMPTY_FIELD_AS_NULL = TRUE;

CREATE STAGE AIRBNB.RAW.S3_RAW
  STORAGE_INTEGRATION = s3_airbnb_int
  URL = 's3://chicago-airbnb-raw-ananya/raw/'
  FILE_FORMAT = AIRBNB.RAW.CSV_GZ;

-- the moment of truth: if this lists your files, the whole IAM handshake works
LIST @AIRBNB.RAW.S3_RAW;
```

If `LIST` errors: 90% of the time it's the trust policy (wrong external ID, or role name doesn't exactly match the integration's ARN). `DESC INTEGRATION s3_airbnb_int;` and re-compare both values.

- [ ] 🔀 **Milestone commit** once `LIST` returns your files — message it explicitly: `storage integration handshake working: LIST returns S3 files`. This is the commit you'll be glad exists if anything IAM-related ever breaks.

## 1.6 Raw tables — `snowflake/setup/04_raw_tables.sql`

**Design decision to internalize:** raw tables land **everything as VARCHAR** plus load metadata. Typing/cleaning happens in dbt staging (Phase 2). Why: a bad value ("$1,250.00" in a NUMBER column) should never kill a *load*; raw preserves the source exactly as received, so you can always reprocess without re-downloading.

`calendar` and `reviews` are narrow — write them by hand:

```sql
-- 04_raw_tables.sql
CREATE TABLE AIRBNB.RAW.CALENDAR (
  listing_id       VARCHAR,
  date             VARCHAR,
  available        VARCHAR,
  price            VARCHAR,
  adjusted_price   VARCHAR,
  minimum_nights   VARCHAR,
  maximum_nights   VARCHAR,
  -- load metadata (every raw table gets these three)
  _source_file     VARCHAR,
  _file_row        NUMBER,
  _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE AIRBNB.RAW.REVIEWS (
  listing_id    VARCHAR,
  id            VARCHAR,
  date          VARCHAR,
  reviewer_id   VARCHAR,
  reviewer_name VARCHAR,
  comments      VARCHAR,
  _source_file  VARCHAR,
  _file_row     NUMBER,
  _loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

`listings` has ~75 columns — don't type them by hand. Generate the DDL from the header:

```bash
gzcat data/raw/2026-06-24/listings.csv.gz | head -1 | python3 -c "
import sys, csv
cols = next(csv.reader(sys.stdin))
body = ',\n'.join(f'  {c} VARCHAR' for c in cols)
print(f'CREATE TABLE AIRBNB.RAW.LISTINGS (\n{body},\n  _source_file VARCHAR,\n  _file_row NUMBER,\n  _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()\n);')
"
```

- [ ] Paste the generated DDL into `04_raw_tables.sql`, run all three CREATEs.

## 1.7 First load with COPY INTO — `snowflake/setup/05_copy_into.sql`

```sql
-- 05_copy_into.sql  (calendar shown; repeat pattern for listings, reviews)
COPY INTO AIRBNB.RAW.CALENDAR (
  listing_id, date, available, price, adjusted_price,
  minimum_nights, maximum_nights, _source_file, _file_row
)
FROM (
  SELECT $1, $2, $3, $4, $5, $6, $7,
         METADATA$FILENAME,          -- which file each row came from
         METADATA$FILE_ROW_NUMBER    -- traceability to the exact source row
  FROM @AIRBNB.RAW.S3_RAW/calendar/
)
FILE_FORMAT = (FORMAT_NAME = AIRBNB.RAW.CSV_GZ)
ON_ERROR = 'ABORT_STATEMENT';
```

For `reviews`, same pattern with `$1..$6` (listing_id, id, date, reviewer_id, reviewer_name, comments). You may include `_loaded_at` explicitly as `CURRENT_TIMESTAMP()` in the SELECT, or omit it and let the column DEFAULT fill it — both work; pick one style for all three tables.

For `listings` (~75 columns), don't hand-type the SELECT — generate the complete COPY statement from your actual header, exactly like the DDL in 1.6:

```bash
gzcat data/raw/2026-06-24/listings.csv.gz | head -1 | python3 -c "
import sys, csv
cols = next(csv.reader(sys.stdin))
col_list = ',\n  '.join(cols)
dollars  = ', '.join(f'\${i+1}' for i in range(len(cols)))
print(f'''COPY INTO AIRBNB.RAW.LISTINGS (
  {col_list},
  _source_file, _file_row
)
FROM (
  SELECT {dollars},
         METADATA\$FILENAME,
         METADATA\$FILE_ROW_NUMBER
  FROM @AIRBNB.RAW.S3_RAW/listings/
)
FILE_FORMAT = (FORMAT_NAME = AIRBNB.RAW.CSV_GZ)
ON_ERROR = 'ABORT_STATEMENT';''')
"
```

Paste the generated statement into `05_copy_into.sql`. Because both the DDL (1.6) and this COPY are generated from the *same header*, the column order can't disagree — that's the point of generating rather than typing. (If a future snapshot vintage changes the header, you regenerate both — runbook failure mode #5.)

- [ ] Run all three COPYs. Check counts: `SELECT COUNT(*) FROM AIRBNB.RAW.CALENDAR;` (~3M).

⚠️ **Break it on purpose #1 — idempotency.** Run the exact same `COPY INTO` again. Result: `0 files processed`. Snowflake's **load history** remembers which files it already loaded per table (14 days for COPY, 64 for Snowpipe) and silently skips them. *This is file-level idempotency, and it's your first concrete answer to "how do you recover from a failed load?" — re-run it; already-loaded files can't duplicate.* Also try `FORCE = TRUE` once to see the counter-example (then `TRUNCATE` and reload cleanly).

⚠️ **Break it on purpose #2 — bad rows.** Make a tiny CSV with a wrong column count, gzip it, upload it under `raw/calendar/dt=9999-01-01/`, and run the COPY once with `ON_ERROR = 'ABORT_STATEMENT'` (whole load fails — nothing partial) and once with `ON_ERROR = 'CONTINUE'` (loads good rows, skips bad; `SELECT * FROM TABLE(VALIDATE(AIRBNB.RAW.CALENDAR, JOB_ID => '_last'));` shows the rejects). Decide and document which you want and why (recommended: `ABORT_STATEMENT` for batch snapshots — all-or-nothing is easier to reason about). Delete the junk file and rows after.

- [ ] 🔀 **Commit** the raw DDL + COPY scripts with the break-it findings in the message: `raw tables + first load; re-run COPY skipped all files (load history dedupe confirmed)`.

## 1.8 Load audit — `snowflake/setup/06_load_audit.sql`

"How do you know last night's load worked?" → you query, not vibe-check:

```sql
-- 06_load_audit.sql
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
```

*(COPY_HISTORY only looks back 14 days — note the limitation, it's an interview detail.)*

- [ ] 🔀 **Commit, then raise PR #2** (`feature/ingestion-snowflake`). In the description, list what exists *outside* the repo that the scripts created or reference: the S3 bucket name, the IAM policy + role names, and which Snowflake objects were built. Infra PRs should be the complete record even when the infrastructure itself isn't in git. Review your diff, merge.

## 1.9 Automate with Snowpipe — `snowflake/pipes/01_pipe_calendar.sql`

Start branch `feature/ingestion-snowpipe` here.

**Concept:** Snowpipe = your `COPY INTO`, wrapped in a serverless pipe that fires automatically when S3 announces a new file (S3 event → SQS queue owned by Snowflake → pipe runs). No warehouse, no schedule — files load within ~a minute of landing.

- [ ] One pipe per entity. Calendar shown; repeat for listings and reviews:

```sql
-- pipes/01_pipe_calendar.sql
CREATE PIPE AIRBNB.RAW.PIPE_CALENDAR
  AUTO_INGEST = TRUE
AS
  COPY INTO AIRBNB.RAW.CALENDAR (
    listing_id, date, available, price, adjusted_price,
    minimum_nights, maximum_nights, _source_file, _file_row
  )
  FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
    FROM @AIRBNB.RAW.S3_RAW/calendar/
  )
  FILE_FORMAT = (FORMAT_NAME = AIRBNB.RAW.CSV_GZ)
  ON_ERROR = 'SKIP_FILE';   -- explicit, though it's also the pipe default
```

*Why not `ABORT_STATEMENT` like the manual COPY? Pipes don't support it — only `CONTINUE`/`SKIP_FILE`. Async ingestion has no watcher to "fail loudly" at, so error handling shifts from prevention-at-load to detection-via-monitoring: a skipped file never shows `Loaded` in LOAD_HISTORY → the Phase 4 sensor times out → Phase 5 emails you. And since the file is the unit of processing, SKIP_FILE is still all-or-nothing per file — the "nothing partial" property survives. Declare it explicitly anyway; implicit defaults are where surprises live.*

- [ ] Get the SQS queue ARN: `SHOW PIPES IN AIRBNB.RAW;` → copy the `notification_channel` value (all pipes on one bucket share it).
- [ ] AWS Console → S3 → your bucket → **Properties** → **Event notifications** → Create:
  - Name: `snowpipe-raw`
  - Prefix: `raw/`  Suffix: `.csv.gz`
  - Event types: **All object create events**
  - Destination: **SQS queue** → "Enter SQS queue ARN" → paste the `notification_channel` ARN.
- [ ] **Test end-to-end:** upload your second snapshot (or re-upload the first under a new `dt=` path):

```bash
SNAP=2025-09-22   # the second snapshot, downloaded in 1.1
for f in listings calendar reviews; do
  aws s3 cp data/raw/$SNAP/$f.csv.gz \
    s3://chicago-airbnb-raw-ananya/raw/$f/dt=$SNAP/$f.csv.gz
done
```

  Wait ~1–2 minutes, then check it arrived with **zero manual steps**:

```sql
SELECT SYSTEM$PIPE_STATUS('AIRBNB.RAW.PIPE_CALENDAR');   -- executionState: RUNNING
SELECT _source_file, COUNT(*) FROM AIRBNB.RAW.CALENDAR GROUP BY 1;  -- both dt= paths present
SELECT * FROM AIRBNB.AUDIT.LOAD_HISTORY ORDER BY last_load_time DESC;
```

## 1.10 Wrap up the phase

- [ ] Write `docs/ingestion.md`: the architecture (S3 event → SQS → Snowpipe), the idempotency guarantees (load history at file level), the `ON_ERROR` decision and why, and how to load a new quarterly snapshot (2 commands: download script + s3 cp).
- [ ] 🔀 **Commit + raise PR #3** (`feature/ingestion-snowpipe`): pipe SQL + docs. The end-to-end test result belongs in the description ("uploaded 2025-09-22 snapshot; all 3 files in Snowflake within ~90s, zero manual steps"). Merge.

**✅ Phase 1 acceptance criteria**
- A file dropped into `s3://…/raw/<entity>/dt=<date>/` appears in Snowflake within ~2 minutes, untouched by human hands.
- Re-running any COPY, or re-notifying the same file, loads nothing twice (prove it: row counts unchanged).
- `AIRBNB.AUDIT.LOAD_HISTORY` answers "what loaded, when, with how many rows, with what errors."
- Every object (warehouse, integration, stage, tables, pipes) exists as a committed SQL file.

**🎤 What you can now say in interviews (and not before):**
- "Ingestion is event-driven: S3 event notifications trigger Snowpipe via SQS; files load within a minute of landing, with no scheduler and no warehouse involved."
- "Loads are idempotent at the file level — Snowflake load history dedupes by file, so recovery from a failed or repeated load is simply re-running it."
- "I chose ABORT_STATEMENT over CONTINUE for batch snapshots and can explain the tradeoff."
- "Every piece of infrastructure is a SQL script in the repo — I can rebuild the account from scratch in half an hour."

---

# Phase 2 — dbt project + staging layer

**Outcome:** a dbt project lives in the repo; the VARCHAR mess in RAW becomes four clean, typed, grain-tested staging views; `dbt build` is green; lineage renders in a browser.

**Prerequisites:** Phase 1 acceptance criteria all pass — Snowpipe loads hands-free, `AUDIT.LOAD_HISTORY` works, RAW holds at least one full snapshot (ideally two).

Work on branch `feature/dbt-staging`.

## 2.0 Concepts + the two design decisions this phase rests on

dbt moves no data — Snowpipe already landed it. dbt runs **SQL transformations inside Snowflake**, in dependency order, with tests. `ref()`/`source()` build the dependency graph (execution order + lineage for free); every model compiles to plain SQL in `target/compiled/` — read it whenever dbt feels like magic.

**Decision 1 — staging = views, marts = tables.** Staging is a thin cleaning layer: views cost nothing to store, stay automatically in sync with RAW, and Snowflake queries them fine at 3M rows. Marts (Phase 3) are expensive to compute and read often → tables/incremental. Your old project had no answer to "why did you materialize it that way?"; now you do.

**Decision 2 — grain is explicit and tested.** RAW accumulates every quarterly snapshot, so grain is per-snapshot (see the worked example we discussed: listing 123 appears once per snapshot, correctly). Every staging model states its grain and enforces it with a combination-uniqueness test. A plain `unique` on `listing_id` would fail the moment snapshot two loads — not bad data, a misstated grain.

## 2.1 Least-privilege dbt user — `snowflake/setup/07_dbt_user.sql`

Never run dbt as ACCOUNTADMIN:

```sql
-- 07_dbt_user.sql  (run as ACCOUNTADMIN)
CREATE ROLE IF NOT EXISTS DBT_ROLE;

GRANT USAGE ON WAREHOUSE TRANSFORM_WH TO ROLE DBT_ROLE;
GRANT USAGE ON DATABASE AIRBNB TO ROLE DBT_ROLE;
GRANT CREATE SCHEMA ON DATABASE AIRBNB TO ROLE DBT_ROLE;    -- dbt creates its schemas itself
GRANT USAGE ON SCHEMA AIRBNB.RAW TO ROLE DBT_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA AIRBNB.RAW TO ROLE DBT_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA AIRBNB.RAW TO ROLE DBT_ROLE;
GRANT USAGE ON SCHEMA AIRBNB.AUDIT TO ROLE DBT_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA AIRBNB.AUDIT TO ROLE DBT_ROLE;

CREATE USER IF NOT EXISTS DBT_USER
  PASSWORD = '<strong-password>'        -- lives in an env var, never in git
  DEFAULT_ROLE = DBT_ROLE
  DEFAULT_WAREHOUSE = TRANSFORM_WH;
GRANT ROLE DBT_ROLE TO USER DBT_USER;
```

*Interview point: DBT_ROLE is read-only on RAW — a bad dbt run **cannot** corrupt landed data, so reprocessing is always possible.*

## 2.2 Install + scaffold

- [ ] From repo root:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install dbt-snowflake
dbt --version
```

- [ ] Append to `.gitignore`: `.venv/`, `dbt/target/`, `dbt/dbt_packages/`, `dbt/logs/`
- [ ] `dbt init dbt` (creates `dbt/` inside the repo — one repo, one project). Delete `dbt/models/example/`.
- [ ] `mkdir -p dbt/models/staging dbt/models/marts dbt/macros`
- [ ] Replace `dbt/dbt_project.yml`:

```yaml
name: chicago_airbnb
version: "1.0.0"
profile: chicago_airbnb

model-paths: ["models"]
test-paths: ["tests"]
snapshot-paths: ["snapshots"]
macro-paths: ["macros"]

clean-targets: ["target", "dbt_packages"]

models:
  chicago_airbnb:
    staging:
      +materialized: view      # Decision 1
      +schema: staging
    marts:
      +materialized: table     # Phase 3 overrides per model where incremental
      +schema: marts
```

## 2.3 Connect — `~/.dbt/profiles.yml` (home directory, NOT the repo)

```yaml
chicago_airbnb:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "<ORGNAME-ACCOUNTNAME>"     # Phase 0.2; format ORG-ACCOUNT, no URL suffix
      user: DBT_USER
      password: "{{ env_var('DBT_SNOWFLAKE_PASSWORD') }}"
      role: DBT_ROLE
      warehouse: TRANSFORM_WH
      database: AIRBNB
      schema: dbt_ananya       # personal dev sandbox; prod target added in Phase 6
      threads: 4
```

- [ ] `export DBT_SNOWFLAKE_PASSWORD='...'` in `~/.zshrc`, then `source ~/.zshrc`.
- [ ] `cd dbt && dbt debug` → all green.

*Dev/prod pattern: in dev, models build into your sandbox (`dbt_ananya_staging`, `dbt_ananya_marts`); prod builds the real schemas. This separation is what makes CI possible in Phase 6.*

- [ ] 🔀 **Commit**: `dbt scaffold + snowflake connection working (dbt debug green)`.

## 2.4 Sources + freshness — `dbt/models/staging/_sources.yml`

```yaml
version: 2

sources:
  - name: airbnb_raw
    database: AIRBNB
    schema: RAW
    loaded_at_field: _loaded_at
    freshness:                  # quarterly cadence: alert when a snapshot is overdue
      warn_after: {count: 100, period: day}
      error_after: {count: 130, period: day}
    tables:
      - name: CALENDAR
      - name: LISTINGS
      - name: REVIEWS
```

- [ ] `dbt source freshness` → passes. This command is the seed of your monitoring story; Phase 5 alerts on it.

## 2.5 Shared macro — `dbt/macros/extract_snapshot_date.sql`

Snapshot date comes from the S3 path (`raw/calendar/dt=2025-09-22/...`). Extract it in exactly one place:

```sql
{% macro extract_snapshot_date(filename_col) %}
    TO_DATE(REGEXP_SUBSTR({{ filename_col }}, 'dt=(\\d{4}-\\d{2}-\\d{2})', 1, 1, 'e', 1))
{% endmacro %}
```

## 2.6 The four staging views — structure, transformations, SQL

### `stg_calendar` — grain: **one row per listing × calendar date × snapshot** (~3M rows/snapshot)

| Column | Type | From (RAW) | Transformation |
|---|---|---|---|
| listing_id | NUMBER | listing_id | `cast` (an unparseable ID *should* fail the build) |
| calendar_date | DATE | date | `to_date` |
| is_available | BOOLEAN | available | `= 't'` |
| price | NUMBER(10,2) | price | strip `$` and `,` → `try_cast` |
| minimum_nights | NUMBER | minimum_nights | `try_cast` |
| maximum_nights | NUMBER | maximum_nights | `try_cast` |
| snapshot_date | DATE | _source_file | macro |
| _loaded_at | TIMESTAMP | _loaded_at | pass-through |

```sql
-- dbt/models/staging/stg_calendar.sql
with source as (
    select * from {{ source('airbnb_raw', 'CALENDAR') }}
)
select
    cast(listing_id as number)                           as listing_id,
    to_date(date)                                        as calendar_date,
    (available = 't')                                    as is_available,
    try_cast(replace(replace(price,'$',''),',','') as number(10,2)) as price,
    try_cast(minimum_nights as number)                   as minimum_nights,
    try_cast(maximum_nights as number)                   as maximum_nights,
    {{ extract_snapshot_date('_source_file') }}          as snapshot_date,
    _loaded_at
from source
```

### `stg_listings` — grain: **one row per listing × snapshot** (~8–9k rows/snapshot)

Carries ~15 of the ~75 RAW columns — only what the marts and the 9 questions need. Verify names against your actual header (they drift between snapshot vintages) and the [Inside Airbnb data dictionary](https://insideairbnb.com/get-the-data/).

| Column | Type | From (RAW) | Transformation |
|---|---|---|---|
| listing_id | NUMBER | id | `cast` |
| listing_name | VARCHAR | name | pass-through |
| neighbourhood | VARCHAR | neighbourhood_cleansed | pass-through (the *_cleansed* one — geocoded, consistent) |
| room_type | VARCHAR | room_type | pass-through |
| property_type | VARCHAR | property_type | pass-through |
| accommodates | NUMBER | accommodates | `try_cast` |
| bedrooms | NUMBER | bedrooms | `try_cast` |
| beds | NUMBER | beds | `try_cast` |
| price | NUMBER(10,2) | price | strip `$`/`,` → `try_cast` |
| minimum_nights | NUMBER | minimum_nights | `try_cast` |
| availability_365 | NUMBER | availability_365 | `try_cast` |
| number_of_reviews | NUMBER | number_of_reviews | `try_cast` |
| review_scores_rating | NUMBER(3,2) | review_scores_rating | `try_cast` |
| host_id | NUMBER | host_id | `cast` (FK to stg_hosts) |
| snapshot_date | DATE | _source_file | macro |

```sql
-- dbt/models/staging/stg_listings.sql
with source as (
    select * from {{ source('airbnb_raw', 'LISTINGS') }}
)
select
    cast(id as number)                                   as listing_id,
    name                                                 as listing_name,
    neighbourhood_cleansed                               as neighbourhood,
    room_type,
    property_type,
    try_cast(accommodates as number)                     as accommodates,
    try_cast(bedrooms as number)                         as bedrooms,
    try_cast(beds as number)                             as beds,
    try_cast(replace(replace(price,'$',''),',','') as number(10,2)) as price,
    try_cast(minimum_nights as number)                   as minimum_nights,
    try_cast(availability_365 as number)                 as availability_365,
    try_cast(number_of_reviews as number)                as number_of_reviews,
    try_cast(review_scores_rating as number(3,2))        as review_scores_rating,
    cast(host_id as number)                              as host_id,
    {{ extract_snapshot_date('_source_file') }}          as snapshot_date
from source
```

### `stg_hosts` — grain: **one row per host × snapshot** (~5–6k rows/snapshot)

**The normalization decision:** host attributes arrive embedded (denormalized) in LISTINGS — a host with 10 listings appears 10 times. Split hosts out and dedupe. `row_number()` (not `select distinct`) guarantees one row even if a host's listings disagree within a snapshot — real-world defensive modeling.

| Column | Type | From (RAW.LISTINGS) | Transformation |
|---|---|---|---|
| host_id | NUMBER | host_id | `cast` |
| host_name | VARCHAR | host_name | pass-through |
| host_since | DATE | host_since | `to_date` |
| host_response_time | VARCHAR | host_response_time | pass-through ('within an hour', …) |
| host_response_rate | NUMBER(4,3) | host_response_rate | strip `%` → `try_cast` → ÷100 (stored 0–1) |
| host_acceptance_rate | NUMBER(4,3) | host_acceptance_rate | same |
| is_superhost | BOOLEAN | host_is_superhost | `= 't'` |
| host_listings_count | NUMBER | host_listings_count | `try_cast` |
| is_identity_verified | BOOLEAN | host_identity_verified | `= 't'` |
| snapshot_date | DATE | _source_file | macro |

```sql
-- dbt/models/staging/stg_hosts.sql
with source as (
    select * from {{ source('airbnb_raw', 'LISTINGS') }}
),
extracted as (
    select
        cast(host_id as number)                          as host_id,
        host_name,
        to_date(host_since)                              as host_since,
        host_response_time,
        try_cast(replace(host_response_rate,'%','') as number)/100   as host_response_rate,
        try_cast(replace(host_acceptance_rate,'%','') as number)/100 as host_acceptance_rate,
        (host_is_superhost = 't')                        as is_superhost,
        try_cast(host_listings_count as number)          as host_listings_count,
        (host_identity_verified = 't')                   as is_identity_verified,
        {{ extract_snapshot_date('_source_file') }}      as snapshot_date,
        row_number() over (
            partition by host_id, {{ extract_snapshot_date('_source_file') }}
            order by _file_row
        ) as rn
    from source
    where host_id is not null
)
select * exclude rn from extracted where rn = 1
```

### `stg_reviews` — grain: **one row per review** (deduped across snapshots)

**Subtlety:** each quarterly reviews file contains the *entire cumulative history*, so loading two snapshots duplicates almost every review. Unlike listings (where per-snapshot rows are meaningful — attributes change), a review is an immutable event; keeping copies adds nothing. So this model dedupes to the latest occurrence — and *that contrast with stg_listings is precisely the kind of grain reasoning interviews probe*.

| Column | Type | From (RAW) | Transformation |
|---|---|---|---|
| review_id | NUMBER | id | `cast` |
| listing_id | NUMBER | listing_id | `cast` |
| review_date | DATE | date | `to_date` |
| reviewer_id | NUMBER | reviewer_id | `try_cast` |
| first_seen_snapshot | DATE | _source_file | macro (kept for auditability) |

`comments` (huge free text) is deliberately not carried — no analytical question uses it, and it stays in RAW if ever needed.

```sql
-- dbt/models/staging/stg_reviews.sql
with source as (
    select * from {{ source('airbnb_raw', 'REVIEWS') }}
),
deduped as (
    select
        cast(id as number)                               as review_id,
        cast(listing_id as number)                       as listing_id,
        to_date(date)                                    as review_date,
        try_cast(reviewer_id as number)                  as reviewer_id,
        {{ extract_snapshot_date('_source_file') }}      as first_seen_snapshot,
        row_number() over (
            partition by cast(id as number)
            order by {{ extract_snapshot_date('_source_file') }}
        ) as rn
    from source
)
select * exclude rn from deduped where rn = 1
```

## 2.7 Tests — `dbt/packages.yml` + `dbt/models/staging/_staging.yml`

- [ ] `dbt/packages.yml`, then `dbt deps`:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

- [ ] `_staging.yml` — every model's grain enforced, every key not-null, try_cast fallout measured at `warn`:

```yaml
version: 2

models:
  - name: stg_calendar
    description: One row per listing per calendar date per snapshot.
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [listing_id, calendar_date, snapshot_date]
    columns:
      - name: listing_id
        data_tests: [not_null]
      - name: price
        data_tests:
          - not_null:
              config: {severity: warn}     # try_cast NULLs = measured signal, not failure

  - name: stg_listings
    description: One row per listing per snapshot.
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [listing_id, snapshot_date]
    columns:
      - name: listing_id
        data_tests: [not_null]
      - name: host_id
        data_tests: [not_null]
      - name: room_type
        data_tests:
          - accepted_values:
              values: ['Entire home/apt', 'Private room', 'Shared room', 'Hotel room']

  - name: stg_hosts
    description: One row per host per snapshot (deduped from listings).
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [host_id, snapshot_date]
    columns:
      - name: host_id
        data_tests: [not_null]

  - name: stg_reviews
    description: One row per review, deduped across snapshots.
    columns:
      - name: review_id
        data_tests: [unique, not_null]
      - name: listing_id
        data_tests: [not_null]
```

## 2.8 Build and inspect

```bash
dbt build                              # models + tests, graph order; prefer over run+test
dbt docs generate && dbt docs serve    # lineage graph at localhost:8080
```

- [ ] Walk the lineage: 3 RAW sources → 4 staging views. This graph grows into source→dashboard by Phase 6.
- [ ] Open one file in `target/compiled/chicago_airbnb/models/staging/` — see exactly what ran.
- [ ] 🔀 **Commit**: `4 staging views + sources + tests; dbt build green`.

## 2.9 ⚠️ Break it on purpose

1. **Duplicate:** insert a copied row into `RAW.LISTINGS` (as ACCOUNTADMIN), `dbt build` → grain test fails **and downstream models are skipped** (graph-aware `build` = bad data can't propagate). Delete, rebuild.
2. **Garbage price:** insert a row with price `'call me'` → `try_cast` yields NULL → warn-severity test reports it *without* failing the build. Severity system, seen not read.
3. **Freshness:** temporarily set `warn_after` to 1 day → `dbt source freshness` warns → revert. You've now seen your future Phase 5 alert fire.

## 2.10 Wrap up

- [ ] `docs/staging.md` — four short sections, each an interview answer: grain of each model (and why reviews dedupes but listings doesn't), try_cast + severity, views vs tables, host normalization.
- [ ] 🔀 **Commit + raise the PR** (`feature/dbt-staging` — one PR covers this whole phase). Description: what the 4 models are, their grains, and the break-it findings from 2.8. Review your diff, merge.

## Where this leads — target star schema (built in Phase 3, designed now)

Staging exists to feed this. Kimball star: facts in the middle, dimensions around them.

```
        dim_host  (SCD Type 2 — from stg_hosts via dbt snapshot)     dim_listing  (from stg_listings)
        ─────────────────────────────                                ─────────────────────────────
        host_key (surrogate)                                         listing_key (surrogate)
        host_id                                                      listing_id
        host_name, host_since                                        listing_name, neighbourhood
        host_response_time / _rate                                   room_type, property_type
        is_superhost          ◄── changes                            accommodates, bedrooms, beds
        valid_from, valid_to      tracked                            minimum_nights, host_id (FK)
        is_current                across                             current price, review score
                    ▲             snapshots                                      ▲
                    │                                                            │
                    └──────────────┐                              ┌──────────────┘
                                   │                              │
                          fct_calendar_day   (from stg_calendar — incremental, merge + lookback)
                          ─────────────────────────────
                          listing_key (FK) · host_key (FK) · calendar_date
                          price · is_available · minimum_nights
                          snapshot_date        grain: listing × date × snapshot
                                   ▲
                                   │
                          fct_reviews  (from stg_reviews)
                          ─────────────────────────────
                          review_id · listing_key (FK) · review_date
                          grain: one review
                                   │
                                   ▼
                          est_occupancy  (derived mart — Inside Airbnb "San Francisco Model")
                          ─────────────────────────────
                          listing_key · month
                          est_bookings   = reviews / 0.50
                          est_nights     = est_bookings × GREATEST(3, minimum_nights)
                          est_occupancy  = LEAST(est_nights / days_in_month, 0.70)
                          est_revenue    = est_nights × avg_price
```

*Mapping to the 9 questions: fct_calendar_day answers Q1–3, 5; est_occupancy answers Q4, 6; dim_host (SCD2) answers Q7–8; listing churn across snapshot_dates in dim_listing/stg_listings answers Q9. The est_* columns are estimates and are always labeled as such — provenance below.*

### Formula provenance (rule: every derived number documents where it came from)

No formula enters a mart without a documented derivation in **`docs/methodology.md`** (created in Phase 3). What's known so far:

**The occupancy model is Inside Airbnb's ["San Francisco Model"](https://insideairbnb.com/data-assumptions/), implemented exactly as published — not our invention:**

| Parameter | Value | Their documented rationale |
|---|---|---|
| Review rate | **50%** | Converts reviews → bookings (not every guest reviews). Airbnb's CEO claimed 72% (unverifiable); SF Budget & Legislative Analyst derived 30.5% from NY Attorney General subpoena data (likely too aggressive — ignored deleted listings). 50% chosen as the midpoint. |
| Length of stay | **GREATEST(3 nights, minimum_nights)** | City-published average where one exists; Chicago has none published, so the 3-night default applies — unless the listing's own minimum_nights is higher, which then wins. |
| Occupancy cap | **70%** | "A well-run hotel" ceiling; keeps the model conservative when reviews lag recent listing changes. |

**Sensitivity analysis (Phase 3 deliverable):** recompute est_revenue at review rates 30.5% / 50% / 72% and show whether neighborhood *rankings* hold even as absolute numbers swing. This demonstrates the conclusions are robust to the shakiest assumption — and it is the strongest possible answer to "these are just estimates, so what?"

**Formulas that are OUR definitions (must be labeled as such, with reasoning in methodology.md):**
- *Pricing gap* (Q2): listing price vs. **median price of its neighbourhood × room_type peer group** — our heuristic, chosen because medians resist luxury-listing outliers; document the peer-group size cutoff (e.g., require ≥5 peers or no gap is reported).
- *Market entry/exit* (Q9): a listing "exited" if present in snapshot N but absent in N+1 — our definition; note the caveat that a delisting and a temporary deactivation are indistinguishable in the data.
- Any future metric follows the same rule: cite the source if adopted, show the reasoning if invented, and label estimates as estimates on the dashboards themselves.

**✅ Phase 2 acceptance criteria**
- `dbt build` green from a fresh clone (given profiles.yml + env var).
- `dbt source freshness` passes; every staging model's grain enforced by a uniqueness test.
- Lineage renders in dbt docs; compiled SQL inspected at least once.
- Decisions documented in `docs/staging.md`; PR merged.

**🎤 What you can now say in interviews:**
- "Staging enforces grain with combination-uniqueness tests — listing × snapshot for listings, but reviews dedupe to one row per review because they're immutable events arriving in cumulative files. Same pipeline, two different grain decisions, both deliberate."
- "try_cast plus warn-severity tests turn malformed values into a measured data-quality signal instead of a 3am failure — and cast stays on IDs, where failure is the right behavior."
- "dbt build is graph-aware: a failed test skips downstream models, so bad data can't propagate to marts."
- "The dbt user is least-privilege, read-only on RAW — a bad transform can never corrupt landed data."

**Deliverables:** dbt project in repo; 1 macro + 4 staging views (specs above); sources with freshness; ~12 tests green; `docs/staging.md`; target star schema documented; PR merged.
**Answers enabled:** none of the 9 yet — this phase buys trustworthy inputs; Phase 3 converts them into answers.

---

# Phase 3 — Marts: dimensional model, SCD2, estimated occupancy

**Outcome:** the analytical heart — the star schema from the Phase 2 diagram, built for real. Every model here maps to at least one of the 9 questions; anything that doesn't, doesn't get built.

**Prerequisites:** Phase 2 acceptance criteria pass; at least two snapshots loaded (SCD2 and market-churn need history — one snapshot works but produces single-version dims).

This phase is **3 PRs** — dims (`feature/marts-dims`, 3.1–3.3), facts (`feature/marts-facts`, 3.4–3.5), derived marts + docs (`feature/marts-derived`, 3.6–3.11). Steps appear inline. New files live in `dbt/models/marts/`.

## 3.0 Concepts you'll use in every model here

**Surrogate keys.** In an SCD2 dim, `host_id` no longer identifies a row (host 456 has one row per version). So each *version* gets a generated key: `host_key = generate_surrogate_key(host_id, valid_from)`. Facts store the surrogate key of the version that was current on the fact's date. This is standard Kimball mechanics and a guaranteed interview topic.

**Unknown members.** When a fact row can't find its dimension row (orphan), the old project's inner joins silently dropped it. Here: dims contain a special `-1` "Unknown" row, facts `LEFT JOIN` and `COALESCE` to `-1`, and a warn-severity test counts how many facts landed there. Nothing vanishes; orphan volume becomes a *measured signal*.

**Versioning trigger columns.** SCD2 creates a new version only when a **tracked column** changes. Choose them deliberately: `number_of_reviews` changes every quarter for every active listing — tracking it would version every row every snapshot and SCD2 degenerates into a per-snapshot copy. Track what's *slowly changing and analytically meaningful*; leave fast-moving measures to facts.

## 3.1 `dim_date` — the calendar spine

Grain: **one row per day**, covering min(review_date) → max(calendar_date). Built with `dbt_utils.date_spine`. Makes month/season logic trivial in SQL and Tableau.

| Column | Type | Derivation |
|---|---|---|
| date_day | DATE | spine |
| year / quarter / month_num | NUMBER | `year()`, `quarter()`, `month()` |
| month_name | VARCHAR | `to_char(date_day,'MMMM')` |
| day_of_week / is_weekend | NUMBER / BOOLEAN | `dayofweek()`, `in (0,6)` |

```sql
-- dbt/models/marts/dim_date.sql
{{ config(materialized='table') }}
with spine as (
    {{ dbt_utils.date_spine(datepart="day",
        start_date="to_date('2010-01-01')",
        end_date="dateadd(day, 400, current_date)") }}
)
select
    cast(date_day as date)            as date_day,
    year(date_day)                    as year,
    quarter(date_day)                 as quarter,
    month(date_day)                   as month_num,
    to_char(date_day, 'MMMM')         as month_name,
    dayofweek(date_day)               as day_of_week,
    dayofweek(date_day) in (0, 6)     as is_weekend
from spine
```

## 3.2 `dim_host` — SCD Type 2, derived in SQL

Grain: **one row per host per version**. Tracked (version-triggering) columns: `is_superhost`, `host_response_time`, `host_response_rate`, `host_acceptance_rate`, `host_listings_count`, `is_identity_verified`. Untracked (carried, value as of version start): `host_name`, `host_since`.

| Column | Type | Derivation |
|---|---|---|
| host_key | VARCHAR | `generate_surrogate_key(host_id, valid_from)` |
| host_id | NUMBER | natural key |
| (attributes) | — | from stg_hosts, value at version start |
| valid_from | DATE | snapshot_date where the version first appeared |
| valid_to | DATE | next version's valid_from; NULL for current |
| is_current | BOOLEAN | `valid_to is null` |

```sql
-- dbt/models/marts/dim_host.sql
{{ config(materialized='table') }}
with hosts as (
    select * from {{ ref('stg_hosts') }}
),
hashed as (   -- one hash over the tracked columns = cheap change detection
    select *,
        {{ dbt_utils.generate_surrogate_key([
            'is_superhost','host_response_time','host_response_rate',
            'host_acceptance_rate','host_listings_count','is_identity_verified'
        ]) }} as attr_hash
    from hosts
),
with_prev as (
    select *,
        lag(attr_hash) over (partition by host_id order by snapshot_date) as prev_attr_hash
    from hashed
),
version_starts as (   -- keep only rows where something tracked actually changed
    select * from with_prev
    where prev_attr_hash is null or attr_hash <> prev_attr_hash
),
versioned as (
    select *,
        snapshot_date                                                          as valid_from,
        lead(snapshot_date) over (partition by host_id order by snapshot_date) as valid_to
    from version_starts
)
select
    {{ dbt_utils.generate_surrogate_key(['host_id','valid_from']) }} as host_key,
    host_id, host_name, host_since, host_response_time, host_response_rate,
    host_acceptance_rate, is_superhost, host_listings_count, is_identity_verified,
    valid_from, valid_to,
    (valid_to is null) as is_current
from versioned

union all   -- the Unknown member: orphan facts coalesce here instead of vanishing
select '-1', null, 'Unknown', null, null, null, null, null, null, null,
       to_date('1900-01-01'), null, true
```

*Read the CTEs in order — hash tracked columns → compare to previous snapshot → keep change rows → window into valid_from/valid_to. That 5-step pattern IS derived SCD2; be able to whiteboard it.*

## 3.3 `dim_listing` — same SCD2 pattern

Grain: **one row per listing per version**.

**How the tracked set was chosen** (this reasoning is the interview answer): a column enters the hash only if (a) some question cares what the value was *at the time*, and (b) it changes slowly enough that tracking it doesn't version every row every snapshot. Labels stay out of the hash but ride along ("untracked" — their value is as-of version start). Measures stay out of the dim entirely. Never hash the natural key or time/metadata columns (`snapshot_date` in the hash = a new "version" every load, the classic SCD2 bug).

| Treatment | Columns | Why |
|---|---|---|
| Tracked (in hash) | neighbourhood, room_type, property_type, accommodates, bedrooms, beds, minimum_nights | History matters (segmentation over time), changes are rare |
| Untracked (carried) | listing_name, host_id | Labels/links; history not analytically interesting |
| Excluded from dim | price, number_of_reviews, availability_365, review_scores_rating | Fast-moving measures — would degenerate SCD2; marts read them from staging per snapshot |

```sql
-- dbt/models/marts/dim_listing.sql
{{ config(materialized='table') }}
with listings as (
    select * from {{ ref('stg_listings') }}
),
hashed as (   -- fingerprint ONLY the tracked columns
    select *,
        {{ dbt_utils.generate_surrogate_key([
            'neighbourhood','room_type','property_type',
            'accommodates','bedrooms','beds','minimum_nights'
        ]) }} as attr_hash
    from listings
),
with_prev as (
    select *,
        lag(attr_hash) over (partition by listing_id order by snapshot_date) as prev_attr_hash
    from hashed
),
version_starts as (
    select * from with_prev
    where prev_attr_hash is null or attr_hash <> prev_attr_hash
),
versioned as (
    select *,
        snapshot_date                                                             as valid_from,
        lead(snapshot_date) over (partition by listing_id order by snapshot_date) as valid_to
    from version_starts
)
select
    {{ dbt_utils.generate_surrogate_key(['listing_id','valid_from']) }} as listing_key,
    listing_id,
    listing_name,
    host_id,
    neighbourhood,
    room_type,
    property_type,
    accommodates,
    bedrooms,
    beds,
    minimum_nights,
    valid_from,
    valid_to,
    (valid_to is null) as is_current
from versioned

union all   -- Unknown member: orphan facts coalesce here instead of vanishing
select '-1', null, 'Unknown', null, null, null, null, null, null, null, null,
       to_date('1900-01-01'), null, true
```

*Documented tradeoff of untracked columns: if only `listing_name` changes between snapshots, no new version is created — the dim shows the name as of the version's start until a tracked change occurs. Fine for labels; that's what untracked means.*

> **📋 Decision record: how to implement SCD2 (decided, Jul 2026)**
>
> **Context:** dim_host needs history — "was this host a superhost *at the time*?" Two ways to build SCD2 in dbt.
>
> **Option A — `dbt snapshot` (the built-in feature).** Runs on a schedule, diffs current source state against what it saw last run, writes valid_from/valid_to. The textbook answer.
> **Option B — derive SCD2 in plain SQL.** Our RAW layer already retains *every* quarterly snapshot, so history exists in the data itself; a model compares consecutive `snapshot_date` rows in stg_hosts and computes valid_from/valid_to/is_current.
>
> **Decision: Option B (derived).**
> **Rationale:** a dbt snapshot table is *stateful* — it only knows what it observed at runtime; drop it and history is unrecoverable. The derived version is **rebuildable from RAW at any time** (`--full-refresh` safe), consistent with the architecture's core promise that a bad transform can never lose data. It's also testable like any model and has no special run-scheduling requirements.
> **Tradeoffs accepted:** Option A captures intra-quarter changes if run more often (irrelevant here — the source *is* quarterly); Option A is less SQL to write; interviewers may expect the built-in feature, so the interview line is: *"dbt snapshots capture state at runtime — but my raw layer already retains full history, so I derived SCD2 in SQL and kept the whole warehouse rebuildable from raw. I'd reach for dbt snapshots when the source is mutable and history only exists if you capture it."* That last clause shows you know when Option A is the right answer.

- [ ] 🔀 **Commit + raise PR #1** (`feature/marts-dims`): dim_date + both SCD2 dims, with SCD2 spot-checked (pick one host with a changed attribute across snapshots; confirm two versions, correct windows). Put that spot-check in the description. Merge.
## 3.4 `fct_calendar_day` — the big fact, incremental done right

Grain: **one row per listing × calendar date × snapshot** (~3M rows/quarter; both June's and September's view of Oct 1 are kept — that's the forward-pricing-evolution decision). Answers Q1–3, 5.

| Column | Type | Derivation |
|---|---|---|
| calendar_day_key | VARCHAR | `generate_surrogate_key(listing_id, calendar_date, snapshot_date)` — the merge key |
| listing_key / host_key | VARCHAR | dim version valid on snapshot_date; `-1` if orphan |
| listing_id | NUMBER | natural key kept for debugging |
| calendar_date | DATE | FK to dim_date |
| is_available / price / minimum_nights | — | from stg_calendar |
| snapshot_date | DATE | which quarterly observation this is |

```sql
-- dbt/models/marts/fct_calendar_day.sql
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'calendar_day_key',
    cluster_by = ['calendar_date']
) }}

with cal as (
    select * from {{ ref('stg_calendar') }}
    {% if is_incremental() %}
      -- LOOKBACK: >= (not >) re-selects the newest snapshot already in the table.
      -- If the previous run died halfway through loading it, this run repairs it;
      -- merge on calendar_day_key means re-processed rows UPDATE, never duplicate.
      where snapshot_date >= (select coalesce(max(snapshot_date), to_date('1900-01-01')) from {{ this }})
    {% endif %}
),
listing_link as (   -- host_id lives on the listing, same snapshot
    select listing_id, host_id, snapshot_date from {{ ref('stg_listings') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['c.listing_id','c.calendar_date','c.snapshot_date']) }}
                                            as calendar_day_key,
    coalesce(dl.listing_key, '-1')          as listing_key,
    coalesce(dh.host_key, '-1')             as host_key,
    c.listing_id,
    c.calendar_date,
    c.is_available,
    c.price,
    c.minimum_nights,
    c.snapshot_date
from cal c
left join listing_link ll
    on c.listing_id = ll.listing_id and c.snapshot_date = ll.snapshot_date
left join {{ ref('dim_listing') }} dl
    on c.listing_id = dl.listing_id
   and c.snapshot_date >= dl.valid_from
   and (c.snapshot_date < dl.valid_to or dl.valid_to is null)
left join {{ ref('dim_host') }} dh
    on ll.host_id = dh.host_id
   and c.snapshot_date >= dh.valid_from
   and (c.snapshot_date < dh.valid_to or dh.valid_to is null)
```

**Why this fixes the old project's bug (write this in docs/methodology.md):** the old `src_bookings` used `created_at >` watermark with no unique key — late rows silently dropped, reruns after partial failure duplicated. Here: (a) merge + `unique_key` makes reruns *repair* instead of duplicate; (b) the `>=` lookback re-covers the in-flight snapshot; (c) LEFT JOIN + Unknown member means orphans are counted, not vanished. File-level idempotency (Phase 1 load history) + row-level idempotency (this merge) = the complete recovery story.

## 3.5 `fct_reviews`

Grain: **one row per review** (already deduped in staging). Kept lean — its job is feeding `est_occupancy` and the reconciliation queries.

Yes, it's nearly a pass-through — and it earns its place for two reasons worth saying out loud: (1) **materialization boundary** — `stg_reviews` is a view whose window-function dedupe re-executes on every query; this table computes it once and downstream aggregations read cheap; (2) **layer contract** — marts read marts; dashboards and analyses never touch staging, so staging can be refactored without breaking anything public. A pass-through model that carries neither of those would deserve deletion.

```sql
-- dbt/models/marts/fct_reviews.sql
{{ config(materialized='table') }}
select
    review_id,
    listing_id,          -- natural key by design; see the simplification note below
    review_date,
    reviewer_id,
    first_seen_snapshot  -- auditability: which snapshot introduced this review
from {{ ref('stg_reviews') }}
```

*Deliberate simplification (document it):* reviews reach back years before our first snapshot, when no dim version existed — so no version-correct `listing_key` is possible for old reviews. Rather than fake it, `fct_reviews` carries the natural `listing_id`, and downstream marts segment by the listing's **current** attributes. Caveat noted in methodology.md.

- [ ] 🔀 **Commit + raise PR #2** (`feature/marts-facts`): both facts. Do break-it #1 (merge idempotency + heal) *before* raising it — the result belongs in the description. Merge.

## 3.6 `est_occupancy` — the San Francisco Model, with sensitivity built in

Grain: **one row per listing × month × review_rate**. All formulas per the Formula provenance section (Inside Airbnb's published model — cited, not invented). Answers Q4–6; the review_rate dimension IS the sensitivity analysis.

| Column | Derivation |
|---|---|
| listing_id, month_start | grain |
| review_rate | 0.305 / 0.50 / 0.72 — cross-joined scenario values |
| reviews_in_month | count from fct_reviews |
| est_bookings | `reviews_in_month / review_rate` |
| avg_min_nights, avg_price | from fct_calendar_day, latest snapshot covering that month |
| est_nights | `est_bookings * greatest(3, avg_min_nights)` |
| est_occupancy | `least(est_nights / days_in_month, 0.70)` |
| est_nights_capped | `est_occupancy * days_in_month` |
| est_revenue | `est_nights_capped * avg_price` |

```sql
-- dbt/models/marts/est_occupancy.sql  (structure; refine when building)
{{ config(materialized='table') }}
with rates as (
    select column1 as review_rate
    from values (0.305), (0.50), (0.72)   -- 30.5% NY-AG-derived / 50% Inside Airbnb / 72% CEO claim
),
monthly_reviews as (
    select listing_id,
           date_trunc('month', review_date) as month_start,
           count(*)                         as reviews_in_month
    from {{ ref('fct_reviews') }}
    group by 1, 2
),
monthly_calendar as (   -- price & min-nights context from the latest observation of each month
    select listing_id,
           date_trunc('month', calendar_date) as month_start,
           avg(price)                          as avg_price,
           avg(minimum_nights)                 as avg_min_nights
    from {{ ref('fct_calendar_day') }}
    qualify snapshot_date = max(snapshot_date) over (partition by listing_id, date_trunc('month', calendar_date))
    group by 1, 2, snapshot_date
),
combined as (
    select r.listing_id, r.month_start, rates.review_rate, r.reviews_in_month,
           c.avg_price, c.avg_min_nights,
           day(last_day(r.month_start))                        as days_in_month,
           r.reviews_in_month / rates.review_rate              as est_bookings
    from monthly_reviews r
    cross join rates
    left join monthly_calendar c
      on r.listing_id = c.listing_id and r.month_start = c.month_start
)
select *,
    est_bookings * greatest(3, coalesce(avg_min_nights, 3))    as est_nights,
    least(est_bookings * greatest(3, coalesce(avg_min_nights, 3)) / days_in_month, 0.70)
                                                                as est_occupancy,
    least(est_bookings * greatest(3, coalesce(avg_min_nights, 3)) / days_in_month, 0.70)
        * days_in_month * coalesce(avg_price, 0)                as est_revenue
from combined
```

Dashboards default to `review_rate = 0.50`; the sensitivity view shows whether neighborhood *rankings* hold across all three rates.

## 3.7 `pricing_gap` — our definition, labeled as ours

Grain: **one row per listing in the latest snapshot** (with a price). Answers Q2. Peer group = `neighbourhood × room_type`; median, not mean — resists luxury-listing outliers; **no gap reported when peers < 5** (too small a group to define "normal" — the cutoff is part of the definition, documented in methodology.md).

Price sources from **`stg_listings`** (the listing's advertised nightly price as of the latest snapshot) — calendar price is retired upstream, per the schema-drift incident.

| Column | Derivation |
|---|---|
| listing_id / listing_key | natural key + current dim version |
| current_price | `price` from stg_listings, latest snapshot |
| peer_median_price | `median(price)` over neighbourhood × room_type |
| peer_count | listings in the peer group |
| price_gap_pct | `(current_price − peer_median_price) / peer_median_price`; NULL if peer_count < 5 |

```sql
-- dbt/models/marts/pricing_gap.sql
{{ config(materialized='table') }}
with latest as (   -- latest snapshot only; a gap is a statement about NOW
    select *
    from {{ ref('stg_listings') }}
    where snapshot_date = (select max(snapshot_date) from {{ ref('stg_listings') }})
      and price is not null
),
peers as (
    select
        neighbourhood,
        room_type,
        median(price) as peer_median_price,
        count(*)      as peer_count
    from latest
    group by 1, 2
)
select
    l.listing_id,
    dl.listing_key,
    l.neighbourhood,
    l.room_type,
    l.price                                            as current_price,
    p.peer_median_price,
    p.peer_count,
    case when p.peer_count >= 5
         then round((l.price - p.peer_median_price) / p.peer_median_price, 4)
    end                                                as price_gap_pct,   -- NULL = "no meaningful peer group"
    l.snapshot_date
from latest l
join peers p
  using (neighbourhood, room_type)
left join {{ ref('dim_listing') }} dl
       on l.listing_id = dl.listing_id and dl.is_current
```

*Two refinements to name if asked, both deliberately skipped for simplicity: the listing's own price is inside its peer median (self-inclusion; negligible at n ≥ 5), and no bedroom/accommodates normalization (a 1-bed and 4-bed in the same peer group). Knowing your metric's limitations is part of owning the definition.*

## 3.8 Tests — `dbt/models/marts/_marts.yml` + `dbt/tests/`

Two kinds of code here. **Generic tests** live in yml; **singular tests** are `.sql` files in the `tests/` folder — each is a query that returns *failing rows* (0 rows = pass). dbt runs both during `dbt build`.

`_marts.yml`:

```yaml
version: 2

models:
  - name: fct_calendar_day
    description: One row per listing per calendar date per snapshot.
    columns:
      - name: calendar_day_key
        data_tests: [unique, not_null]
      - name: listing_key
        data_tests:
          - relationships:
              arguments: {to: ref('dim_listing'), field: listing_key}
      - name: host_key
        data_tests:
          - relationships:
              arguments: {to: ref('dim_host'), field: host_key}

  - name: dim_host
    columns:
      - name: host_key
        data_tests: [unique, not_null]

  - name: dim_listing
    columns:
      - name: listing_key
        data_tests: [unique, not_null]

  - name: est_occupancy
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: [listing_id, month_start, review_rate]

  - name: fct_reviews
    columns:
      - name: review_id
        data_tests: [unique, not_null]
```

*(The relationships tests pass **because** the Unknown `-1` row exists in each dim — orphans resolve to it instead of failing the test or vanishing. The orphan volume itself is monitored by the singular warn test below.)*

`tests/assert_scd2_no_overlap.sql` — THE correctness proof for derived SCD2. Checks all three invariants for both dims: windows must be contiguous (no overlap, no gap) and each entity has exactly one current row:

```sql
with dims as (
    select 'dim_host' as dim, host_id as nk, valid_from, valid_to, is_current
    from {{ ref('dim_host') }} where host_id is not null
    union all
    select 'dim_listing', listing_id, valid_from, valid_to, is_current
    from {{ ref('dim_listing') }} where listing_id is not null
),
window_check as (
    select *,
        lead(valid_from) over (partition by dim, nk order by valid_from) as next_valid_from
    from dims
),
bad_windows as (      -- valid_to must exactly equal the next version's valid_from
    select dim, nk, 'window overlap/gap' as problem
    from window_check
    where next_valid_from is not null
      and (valid_to is null or valid_to <> next_valid_from)
),
bad_current as (      -- exactly one is_current row per entity
    select dim, nk, 'current-row count <> 1' as problem
    from dims
    group by dim, nk
    having count_if(is_current) <> 1
)
select * from bad_windows
union all
select * from bad_current
```

`tests/assert_positive_prices.sql` (warn — a signal, not a stop):

```sql
{{ config(severity = 'warn') }}
select listing_id, price, snapshot_date
from {{ ref('stg_listings') }}
where price <= 0
```

`tests/assert_occupancy_bounds.sql` (the cap is part of the published model — violating it means the formula is implemented wrong, so this one errors):

```sql
select *
from {{ ref('est_occupancy') }}
where est_occupancy < 0 or est_occupancy > 0.70
```

`tests/assert_orphan_share_low.sql` (warn) — orphan volume as a measured signal, replacing the old project's silent inner-join loss:

```sql
{{ config(severity = 'warn') }}
select
    count_if(listing_key = '-1') as orphan_rows,
    count(*)                     as total_rows,
    orphan_rows / total_rows     as orphan_share
from {{ ref('fct_calendar_day') }}
having orphan_share > 0.01      -- warn if >1% of facts can't find their dim
```

## 3.9 ⚠️ Break it on purpose

1. **Prove merge idempotency:** run `dbt build --select fct_calendar_day` twice; row count identical both times. Then delete half the newest snapshot's rows straight from the table (simulate a died-midway run) and run again → the lookback + merge **heals it**. Time it. This is your recovery demo, on camera if you like.
2. **Watch SCD2 version:** update one host's `host_is_superhost` in RAW for the latest snapshot, rebuild → a new version row appears with correct valid_from/valid_to; the no-overlap test still passes. Revert.
3. **Orphan a fact:** insert a calendar row for a nonexistent listing_id → it lands with `listing_key = '-1'`, the warn test reports it, nothing disappears.

## 3.10 Reconcile against the source's published numbers

Inside Airbnb's own Chicago page publishes estimates from the same San Francisco Model (at time of writing: **116 avg nights booked, $35,123 avg income**, over 5,796 "recent and frequently booked"-filtered listings). Since you implement the same model, your marts should reproduce their ballpark:

- [ ] Write `analyses/reconciliation_inside_airbnb.sql`. (`analyses/` files are compiled by dbt — Jinja and `ref()` work — but never executed by `dbt build`; run via `dbt compile` + paste the compiled SQL into Snowsight, or `dbt show -s analysis:reconciliation_inside_airbnb`.) Two comparisons, weakest to strongest:

```sql
-- analyses/reconciliation_inside_airbnb.sql
-- Reconcile our San Francisco Model implementation against the provider's own numbers.
-- Adjust column names to the final est_occupancy model if they drifted.

-- PART A: citywide averages vs the numbers on insideairbnb.com/chicago
-- (screenshot the page as of your snapshot date; theirs updates).
with trailing_12m as (
    select listing_id,
           sum(est_nights)  as nights_12m,     -- use the capped nights column if named differently
           sum(est_revenue) as revenue_12m
    from {{ ref('est_occupancy') }}
    where review_rate = 0.50
      and month_start >= dateadd(month, -12, date_trunc(month, current_date))
    group by listing_id
)
select count(*)              as listings,
       round(avg(nights_12m))  as avg_nights_booked,   -- theirs: 116
       round(avg(revenue_12m)) as avg_income           -- theirs: $35,123
from trailing_12m;

-- PART B (stronger): listing-level comparison vs the provider's per-listing
-- estimates shipped in the 2026 vintage (estimated_occupancy_l365d / estimated_revenue_l365d).
with ours as (
    select listing_id,
           sum(est_nights)  as our_nights_12m,
           sum(est_revenue) as our_revenue_12m
    from {{ ref('est_occupancy') }}
    where review_rate = 0.50
      and month_start >= dateadd(month, -12, date_trunc(month, current_date))
    group by listing_id
),
theirs as (
    select cast(id as number)                            as listing_id,
           try_cast(estimated_occupancy_l365d as number) as their_occupancy_365,
           try_cast(estimated_revenue_l365d  as number)  as their_revenue_365
    from {{ source('airbnb_raw', 'LISTINGS') }}
    where _source_file like '%dt=2026-06-24%'
)
select count(*)                                               as compared_listings,
       round(corr(o.our_revenue_12m, t.their_revenue_365), 3) as revenue_correlation,
       round(median(abs(o.our_revenue_12m - t.their_revenue_365)
             / nullif(t.their_revenue_365, 0)), 3)            as median_abs_pct_diff
from ours o
join theirs t using (listing_id)
where t.their_revenue_365 > 0;
```

  *Reading the results: Part A within ±15% of the published citywide numbers, or Part B correlation ≳ 0.9, means your implementation reproduces the provider's methodology. A bigger gap is a diagnosis exercise, not a failure — likely suspects: their "recent and frequently booked" display filter (their listing count on the page vs yours tells you if scopes differ), a city-specific average-stay parameter, or review-window differences. Write up whichever outcome you get in methodology.md.*
- [ ] Compare against the numbers on [their Chicago map page](https://insideairbnb.com/chicago) *as of your snapshot date* (screenshot it — the page updates).
- [ ] Document the result in `docs/methodology.md`: within ~±15% → your pipeline independently reproduces the provider's methodology from raw files. Larger gap → investigate (their "recent and frequently booked" filter, listing scope, city avg-stay parameter) and write up the cause. **Either outcome is the deliverable** — a match proves correctness; a diagnosed mismatch proves you can reconcile, which is the rarer skill.

*Interview line: "I validated my occupancy marts by reconciling against the data provider's published estimates — same methodology, independent implementation, landed within N%."*

## 3.11 Wrap up

- [ ] **`docs/methodology.md`** — every formula per the Formula provenance rule: SF Model (cited, parameters explained), pricing gap + peer cutoff (ours, reasoned), market entry/exit definition (ours, caveated), the fct_reviews current-attribution simplification, and the old-project-bug → merge/lookback fix narrative.
- [ ] **`docs/data_model.md`** — the star schema diagram (copy from Phase 2 section, now as-built), each table's grain, and the tracked-columns list per dim with the *why*.
- [ ] 🔀 **Commit + raise PR #3** (`feature/marts-derived`): est_occupancy, pricing_gap, tests, the 9 question queries, reconciliation write-up, both docs. Merge.

**✅ Phase 3 acceptance criteria**
- `dbt build` green including relationship, grain, SCD2-no-overlap, and bounds tests.
- Break-it #1 performed: partial-failure heal demonstrated and timed.
- Each of the 9 questions answerable with a documented SQL query (put them in `analyses/` — one file per question, `q1_seasonal_pricing.sql` … `q9_market_churn.sql`).
- Reconciliation vs Inside Airbnb's published estimates done and written up (match or diagnosed mismatch — both count).
- methodology.md and data_model.md complete.

**🎤 What you can now say in interviews:**
- "My dims are SCD Type 2, derived in SQL rather than dbt snapshots — my raw layer retains full history, so the warehouse stays rebuildable from raw. I'd use dbt snapshots when the source is mutable and history only exists if captured at runtime."
- "The incremental fact merges on a surrogate of listing × date × snapshot with a lookback window — a partially failed run is repaired by re-running, not duplicated. I tested that by deleting half a snapshot mid-table and re-running."
- "Facts never lose orphans: LEFT JOIN to an Unknown member, with a warn-severity test measuring orphan volume — my earlier project inner-joined and silently dropped them; this one measures them."
- "Revenue figures are estimates from Inside Airbnb's published San Francisco Model — 50% review rate, 3-night default stay, 70% occupancy cap — and I ran the sensitivity at 30.5% and 72% to show neighborhood rankings are robust to the weakest assumption."

**Deliverables:** dim_date, dim_host (SCD2), dim_listing (SCD2), fct_calendar_day (incremental merge), fct_reviews, est_occupancy (with sensitivity), pricing_gap; ~15 new tests; 9 question queries in `analyses/`; methodology.md; data_model.md; PR merged.
**Answers enabled:** all 9 questions, in SQL. Dashboards visualize them in Phase 6.

---

# Phase 4 — Orchestration: Airflow + Cosmos

**Outcome:** nothing runs from your laptop terminal anymore. A quarterly DAG downloads → uploads → verifies Snowpipe → runs the dbt graph model-by-model; a daily DAG heartbeat-checks freshness. Failures retry safely because every layer beneath is idempotent.

**Prerequisites:** Phases 1–3 acceptance criteria pass; Docker Desktop installed (Phase 0.3) and running.

Work on branch `feature/orchestration`.

**The honesty position (state it in README and interviews):** Airflow is overkill for 3M rows/quarter — a scheduled GitHub Action could run this. It's used deliberately to practice production failure semantics: retries, sensors, per-model visibility. Judgment > tooling.

## 4.1 Install the Astro CLI and scaffold

Astro CLI = free Astronomer tool that runs real Airflow locally in Docker with one command.

- [ ] `brew install astro`
- [ ] From repo root:

```bash
mkdir airflow && cd airflow
astro dev init        # scaffolds Dockerfile, dags/, requirements.txt, etc.
```

- [ ] `airflow/requirements.txt`:

```
astronomer-cosmos[dbt-snowflake]
apache-airflow-providers-snowflake
boto3
```

- [ ] Append to repo `.gitignore`: `airflow/.env` (already covered by `.env` — verify).
- [ ] First boot: `astro dev start` → UI at http://localhost:8080 (login `admin`/`admin`). Expect a couple of example DAGs; you'll delete them.

*If `astro dev start` fails: Docker Desktop isn't running, or port 8080 is taken (`astro config set webserver.port 8081`).*

## 4.2 Give the container access to dbt, Snowflake, and AWS

**dbt project into the container.** The dbt project lives at repo root (`../dbt` relative to `airflow/`), outside Astro's build context. Mount it with `airflow/docker-compose.override.yml` (Astro picks this file up automatically):

```yaml
version: "3.1"
services:
  scheduler:
    volumes:
      - ../dbt:/usr/local/airflow/dbt
  webserver:
    volumes:
      - ../dbt:/usr/local/airflow/dbt
  triggerer:
    volumes:
      - ../dbt:/usr/local/airflow/dbt
```

**Credentials via `airflow/.env`** (never committed; Astro injects it into containers):

```
SNOWFLAKE_ACCOUNT=<ORGNAME-ACCOUNTNAME>
DBT_SNOWFLAKE_PASSWORD=<the DBT_USER password>
AWS_ACCESS_KEY_ID=<from Phase 0.1>
AWS_SECRET_ACCESS_KEY=<from Phase 0.1>
AWS_DEFAULT_REGION=us-east-1
# Airflow connection for the Snowpipe sensor, defined as a URI:
AIRFLOW_CONN_SNOWFLAKE_DEFAULT=snowflake://DBT_USER:<password>@/?account=<ORGNAME-ACCOUNTNAME>&database=AIRBNB&warehouse=TRANSFORM_WH&role=DBT_ROLE
```

**A container-friendly profiles.yml** — same pattern as Phase 2 but every value from env vars, safe to commit. Create `airflow/include/profiles.yml`:

```yaml
chicago_airbnb:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: DBT_USER
      password: "{{ env_var('DBT_SNOWFLAKE_PASSWORD') }}"
      role: DBT_ROLE
      warehouse: TRANSFORM_WH
      database: AIRBNB
      schema: dbt_ananya
      threads: 4
```

- [ ] `astro dev restart`, then verify inside the container:

```bash
astro dev bash
ls /usr/local/airflow/dbt          # your dbt project is visible
dbt debug --project-dir /usr/local/airflow/dbt --profiles-dir /usr/local/airflow/include
```

- [ ] 🔀 **Commit**: `astro env boots; dbt reachable inside container (debug green)`.

## 4.3 The quarterly DAG — `airflow/dags/snapshot_load.py`

Four stages: **download+upload → sense Snowpipe completion → dbt graph via Cosmos → notify.** The sensor is the new concept: Snowpipe is asynchronous — you *never assume* it finished, you poll the audit trail until it provably did.

```python
"""Quarterly Inside Airbnb snapshot: download -> S3 -> (Snowpipe) -> verify -> dbt."""
import gzip
import urllib.request
from datetime import timedelta

import boto3
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sensors.python import PythonSensor
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

BUCKET = "chicago-airbnb-raw-ananya"
ENTITIES = ["listings", "calendar", "reviews"]
BASE_URL = "https://data.insideairbnb.com/united-states/il/chicago/{date}/data/{entity}.csv.gz"

DBT_PROJECT_DIR = "/usr/local/airflow/dbt"
PROFILES_PATH = "/usr/local/airflow/include/profiles.yml"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    # "email": ["you@example.com"], "email_on_failure": True,   # wired up in Phase 5
}

profile_config = ProfileConfig(
    profile_name="chicago_airbnb",
    target_name="dev",
    profiles_yml_filepath=PROFILES_PATH,
)


@dag(
    schedule=None,                      # quarterly snapshots arrive irregularly -> manual trigger
    catchup=False,
    default_args=default_args,
    params={"snapshot_date": Param("2026-06-24", type="string")},
    tags=["airbnb", "load"],
)
def snapshot_load():

    @task
    def download_and_upload(**context) -> str:
        """Idempotent by design: same file -> same S3 key -> Snowflake load history skips it."""
        snap = context["params"]["snapshot_date"]
        s3 = boto3.client("s3")
        for entity in ENTITIES:
            url = BASE_URL.format(date=snap, entity=entity)
            local = f"/tmp/{entity}.csv.gz"
            urllib.request.urlretrieve(url, local)
            with gzip.open(local) as f:      # cheap sanity check before shipping
                f.read(1024)
            s3.upload_file(local, BUCKET, f"raw/{entity}/dt={snap}/{entity}.csv.gz")
        return snap

    def _snowpipe_done(snap: str) -> bool:
        """Poke: all 3 files for this snapshot show LOADED in copy history."""
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        rows = hook.get_first(f"""
            select count(distinct table_name)
            from AIRBNB.AUDIT.LOAD_HISTORY
            where status = 'Loaded' and file_name like '%dt={snap}%'
        """)
        return rows[0] >= 3

    snap = download_and_upload()

    wait_for_snowpipe = PythonSensor(
        task_id="wait_for_snowpipe",
        python_callable=_snowpipe_done,
        op_args=[snap],
        poke_interval=60,      # check every minute
        timeout=1800,          # give up after 30 min -> task fails -> alert (Phase 5)
        mode="reschedule",     # frees the worker slot between pokes
    )

    dbt_build = DbtTaskGroup(   # Cosmos: every model+test = its own task, graph mirrors dbt lineage
        group_id="dbt_build",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=ExecutionConfig(),
    )

    @task
    def notify(snap_date: str):
        print(f"snapshot {snap_date} loaded and transformed")  # Phase 5: email summary

    snap >> wait_for_snowpipe >> dbt_build >> notify(snap)


snapshot_load()
```

Design notes to internalize (they're the interview content):
- **`schedule=None` + a date Param** — Inside Airbnb publishes irregularly, so pretending it's a cron is dishonest; a parameterized manual trigger *is* the correct schedule. Alternatively add a weekly "check for new snapshot" probe later.
- **`mode="reschedule"`** on the sensor: between pokes the worker slot is released. With `mode="poke"` a worker sits blocked for up to 30 minutes. In a 3-task toy it doesn't matter; in a 500-task production Airflow it's the difference between fine and gridlock. Know both modes.
- **Retries are safe end-to-end**: re-download → same S3 key → load history skips; re-run dbt → merge repairs. Orchestration-level retries are only safe because Phases 1 and 3 made every layer idempotent. This sentence ties your whole architecture together.

## 4.4 The daily heartbeat — `airflow/dags/freshness_check.py`

```python
"""Daily: is anything silently broken? dbt source freshness against RAW."""
from datetime import timedelta
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

default_args = {"retries": 1, "retry_delay": timedelta(minutes=5)}


@dag(schedule="0 7 * * *", catchup=False, default_args=default_args, tags=["airbnb", "monitoring"])
def freshness_check():
    BashOperator(
        task_id="dbt_source_freshness",
        bash_command=(
            "dbt source freshness "
            "--project-dir /usr/local/airflow/dbt "
            "--profiles-dir /usr/local/airflow/include"
        ),
    )


freshness_check()
```

Quarterly cadence means the load DAG rarely runs — this daily DAG is what makes the pipeline *feel* monitored. It exits non-zero when freshness errors, the task fails, and Phase 5 turns that failure into an email.

## 4.5 Run it for real

- [ ] `astro dev restart` → UI → both DAGs visible, no import errors (check the top banner).
- [ ] Trigger `snapshot_load` with a snapshot_date you *haven't* loaded (or re-trigger an existing one — watch idempotency do its thing: Snowpipe skips the files, sensor still passes because history shows Loaded, dbt merge changes nothing).
- [ ] Watch the Cosmos task group expand: every staging view, dim, and fact is its own green box, tests attached. **Screenshot this** for the README.
- [ ] Unpause `freshness_check`; confirm the next morning's run went green.
- [ ] 🔀 **Commit**: `both DAGs green end-to-end` (include the screenshot in this commit).

## 4.6 ⚠️ Break it on purpose

1. **Kill a model mid-run:** while `fct_calendar_day` runs, mark the task failed (UI). Watch: only it and its *downstream* go red; upstream stays green. Clear the failed task → Airflow re-runs from exactly there, and merge makes it safe. Compare mentally against the single-BashOperator design, which would re-run everything blind.
2. **Starve the sensor:** trigger with a snapshot_date that doesn't exist on S3 → sensor pokes 30 min → times out → task fails. That failure is your Phase 5 alert trigger. Note how *timeout choice is an SLA statement*: "if ingestion hasn't landed in 30 minutes, a human should know."
3. **Break a test:** insert the Phase 2 duplicate again, trigger the DAG → the test task fails, downstream models skip — the same graph-aware protection, now visible as red/orange boxes in a UI instead of terminal output.

## 4.7 Wrap up

- [ ] `docs/orchestration.md`: the two-DAG design and why, sensor semantics (poke vs reschedule, timeout-as-SLA), the Cosmos task-per-model tradeoff (visibility + surgical retries vs slower total runtime and scheduler overhead — at this scale, visibility wins), and the "retries are safe because everything beneath is idempotent" argument.
- [ ] README: add the green-DAG screenshot + the honesty position from the top of this phase.
- [ ] 🔀 **Commit + raise the PR** (`feature/orchestration` — one PR for the phase). Description: DAG design, break-it findings, and the note that `.env` (credentials) is deliberately absent from the diff. Merge.

**✅ Phase 4 acceptance criteria**
- `astro dev start` from a fresh clone (plus `.env`) brings up Airflow with both DAGs importable.
- A full `snapshot_load` run completes green end-to-end, including the sensor actually gating on load history.
- Break-it #1 performed: mid-graph failure recovered by clearing one task, not re-running the world.
- `freshness_check` has at least one scheduled (not manually triggered) green run.

**🎤 What you can now say in interviews:**
- "The DAG never assumes ingestion finished — a reschedule-mode sensor polls Snowflake's load history until all files show Loaded, with a 30-minute timeout that doubles as an SLA."
- "Cosmos renders each dbt model as its own Airflow task, so a failure is visible at model granularity and recovery is clearing one task — not re-running a 20-minute black box."
- "Orchestration retries are only safe because every layer beneath is idempotent — file-level via Snowflake load history, row-level via merge on a surrogate key. I can walk through what happens if the DAG dies at any task."
- "Airflow is honestly overkill at this volume — I chose it to practice production failure semantics, and at this scale I'd defend a scheduled GitHub Action instead."

**Deliverables:** `airflow/` (Astro project, override mount, committed env-var profiles.yml, 2 DAGs); green DAG screenshot in README; `docs/orchestration.md`; PR merged.

---

# Phase 5 — Reliability: email alerting, runbook, recovery drill

**Outcome:** the direct answer to the feedback that started this rebuild — when something breaks, you get an email; when you get an email, the runbook tells you exactly what to do; and you've *drilled* the recovery, with a time to show for it.

**Prerequisites:** Phase 4 acceptance criteria pass (both DAGs green).

This phase is **2 PRs** — `feature/alerting` (5.1–5.3) and `feature/runbook` (5.4–5.5); steps appear inline.

## 5.0 The concept: monitoring vs alerting, and the fatigue rule

Monitoring = you *can* find out (audit views, task history, freshness runs). Alerting = you're *told*. The design failure to avoid is alerting on everything — an inbox full of noise gets ignored, and then the one real failure is missed. So the split, explicitly: **errors alert** (failed loads, failed models, failed error-severity tests, sensor timeouts, stale sources), **warnings don't** (try_cast NULL counts, orphan-key counts — they surface in logs/reports and get reviewed when you're already looking). Deciding what *doesn't* page you is the mature half of the design; write it down in the runbook.

**Coverage insight (why this phase is so cheap):** Cosmos renders every dbt model *and every dbt test* as its own Airflow task. So "email me when any task fails" — one SMTP config — automatically covers: download failures, sensor timeouts, model build failures, test failures, and freshness breaches. No extra observability tooling needed at this scale; know that heavier options (anomaly detection, test-history dashboards) exist and name them as the upgrade path if asked.

## 5.1 SMTP setup — Gmail app password

- [ ] Gmail → manage your Google Account → Security → enable **2-Step Verification** (required first) → then **App passwords** → create one named `airflow` → copy the 16-character password.
- [ ] Append to `airflow/.env` (never committed):

```
AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_STARTTLS=True
AIRFLOW__SMTP__SMTP_SSL=False
AIRFLOW__SMTP__SMTP_USER=<your@gmail.com>
AIRFLOW__SMTP__SMTP_PASSWORD=<the 16-char app password>
AIRFLOW__SMTP__SMTP_MAIL_FROM=<your@gmail.com>
```

- [ ] `astro dev restart`.

## 5.2 Wire alerts into both DAGs

In `snapshot_load.py` and `freshness_check.py`, update `default_args`:

```python
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "email": ["<your@gmail.com>"],
    "email_on_failure": True,     # any task exhausts retries -> email with task, DAG, log link
    "email_on_retry": False,      # DELIBERATE: retries are normal, not incidents. Don't train
                                  # yourself to ignore alerts. (Fatigue rule in action.)
}
```

Optionally, replace the `notify` task in `snapshot_load` with a success summary — defensible because the DAG runs quarterly (low volume); a daily success email would violate the fatigue rule:

```python
from airflow.operators.email import EmailOperator

notify = EmailOperator(
    task_id="notify",
    to=["<your@gmail.com>"],
    subject="airbnb pipeline: snapshot {{ params.snapshot_date }} loaded",
    html_content="Load + dbt build green. Audit: query AIRBNB.AUDIT.LOAD_HISTORY.",
)
```

## 5.3 Prove the alert fires

- [ ] Trigger `snapshot_load` with `snapshot_date=1999-01-01` (nothing to download → task fails after retries) → **email arrives** naming DAG, task, execution time, with a log link. Screenshot it for the README.
- [ ] Check the failure email renders usefully on your phone — that's where you'd actually read it.
- [ ] 🔀 **Commit + raise PR #1** (`feature/alerting`): DAG default_args changes + the screenshot. Note in the description that SMTP config lives in `.env`, not the diff. Merge.

## 5.4 `docs/RUNBOOK.md` — the failure-mode catalog

The document interviewers almost never see in a portfolio. One table plus a backfill section. Every row is a failure you have *personally caused* in a break-it exercise (that's what makes the recovery steps trustworthy):

| # | Failure mode | Detection (what you receive) | Impact | Recovery |
|---|---|---|---|---|
| 1 | Malformed/truncated file | COPY/pipe fails; email from failed sensor or `ABORT_STATEMENT` | No partial data (all-or-nothing per file) | Fix/re-export file → re-upload under same `dt=` key is skipped by load history → upload corrected file under same path with new name, or `ALTER PIPE ... REFRESH`; verify in LOAD_HISTORY |
| 2 | Model/test failure mid-graph | Email names the exact Cosmos task | Downstream models skipped — marts stale but not wrong | Fix root cause → clear the failed task in Airflow → graph resumes from there; merge makes re-runs safe |
| 3 | Sensor timeout (Snowpipe never completed) | Email from `wait_for_snowpipe` after 30 min | dbt never ran; warehouse untouched | Check `SYSTEM$PIPE_STATUS`, S3 event config, file actually landed; fix; re-trigger DAG (idempotent end-to-end) |
| 4 | Stale source (no new snapshot loaded in >100 days) | Email from daily `freshness_check` | Dashboards aging, nothing broken | Check Inside Airbnb for new snapshot date; run `snapshot_load` with it |
| 5 | Schema drift (new snapshot vintage renames/adds columns) | COPY fails (column count) or staging test fails | New snapshot blocked; old data intact | Diff new header vs `04_raw_tables.sql` → add/rename columns in RAW DDL + staging model → reload that snapshot only |
| 6 | Snowflake trial expired | Everything fails; login fails | Total outage, zero data loss (S3 + repo hold everything) | New trial account → run `snowflake/setup/*.sql` in order → re-upload snapshots from `data/raw/` → full rebuild. ~30–45 min, tested |

Backfill section: how to load a missed/old snapshot (`snapshot_load` with the old date — merge + load history make ordering irrelevant), and when `--full-refresh` is justified (schema change in a mart, corrected logic) vs wasteful (routine failures — never).

## 5.5 ⚠️ The drill (capstone of the phase)

Simulate failure #1 or #5 end-to-end without looking at the solution: hand-craft a truncated `calendar.csv.gz`, upload it under a new `dt=`, let the pipeline choke, then **follow your own runbook** — only the runbook — to full recovery. Time it. Record the time in the runbook ("last drilled: <date>, recovery: <N> min"). If the runbook was missing a step, that's the real finding: fix the runbook, not just the pipeline.

This mirrors the 30-minute-recovery claim from your Oracle production-support experience — same muscle, now on a system you built alone.

- [ ] 🔀 **Commit + raise PR #2** (`feature/runbook`): RUNBOOK.md with the drill time recorded. Description: which failure mode you drilled and what the runbook was missing (if anything). Merge.

**✅ Phase 5 acceptance criteria**
- A deliberately failed task produces an email (screenshot in README).
- `email_on_retry` is False and you can say why.
- RUNBOOK.md covers ≥6 failure modes, each personally caused at least once.
- The drill is done and timed; the time is written in the runbook.

**🎤 What you can now say in interviews:**
- "Every model and every test is an Airflow task, so one alerting config covers ingestion, transforms, tests, and freshness — failures email me with the exact task and log link."
- "Retries don't alert — deliberately. Retries are normal; alert fatigue is how real failures get missed."
- "The runbook has six failure modes, each with detection, impact, and recovery — and each one I've personally caused and recovered from. Last drill took me N minutes end to end."
- "Worst case — the whole Snowflake account disappears — is a documented 45-minute rebuild, because every object is a script in the repo and raw data lives in S3."

**Deliverables:** SMTP config; alerting in both DAGs; failure-email screenshot; `docs/RUNBOOK.md` with drill time; 2 PRs merged.

---

# Phase 6 — CI/CD, docs, dashboards

**Outcome:** broken changes physically cannot reach `main`; merges deploy automatically; lineage is a public URL; the 9 questions get visual answers; and — last of all — the resume bullets get written from what exists.

**Prerequisites:** Phases 1–5 acceptance criteria pass.

This phase is **2 PRs** — `feature/ci` (6.1–6.6) and `feature/dashboards` (6.7–6.9); steps appear inline.

## 6.0 CI/CD in plain terms (you're new to this — start here)

**CI (Continuous Integration):** every time you open a PR, a robot builds and tests your changes *before* a human can merge them. **CD (Continuous Deployment):** every time a PR merges to `main`, a robot deploys — here, runs dbt against production schemas and republishes the docs site. The robot is **GitHub Actions**: workflows defined as YAML files in `.github/workflows/`, run by GitHub for free on public repos.

**How your merge ritual changes:** same loop as always (branch → commit → push → PR), but now the PR page shows checks running. Green checkmark → merge as usual. Red ✗ → GitHub blocks the merge button (once we enable branch protection); you push a fix commit to the same branch, checks rerun. That's the entire mental model: *the robot reviews first.*

**What our CI does (kept deliberately simple):** lint the SQL, then `dbt build` the whole project into a throwaway CI schema. At ~15 models this takes a few minutes — no need for anything cleverer. (Real teams with 500 models use state-comparison tricks to build only what changed; know that exists as the scaling answer, nothing more.)

## 6.1 CI/prod targets — extend `airflow/include/profiles.yml`

Add two outputs to the existing file (env-var-driven, safe to commit):

```yaml
    ci:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: DBT_USER
      password: "{{ env_var('DBT_SNOWFLAKE_PASSWORD') }}"
      role: DBT_ROLE
      warehouse: TRANSFORM_WH
      database: AIRBNB
      schema: ci            # throwaway: ci_staging, ci_marts
      threads: 4
    prod:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: DBT_USER
      password: "{{ env_var('DBT_SNOWFLAKE_PASSWORD') }}"
      role: DBT_ROLE
      warehouse: TRANSFORM_WH
      database: AIRBNB
      schema: analytics     # the real thing: analytics_staging, analytics_marts
      threads: 4
```

Point the Phase 4 Airflow DAG's Cosmos `ProfileConfig` at `target_name="prod"` from now on — Airflow *is* production.

## 6.2 GitHub secrets

Repo → Settings → Secrets and variables → Actions → New repository secret:

- `SNOWFLAKE_ACCOUNT` = your account identifier
- `DBT_SNOWFLAKE_PASSWORD` = the DBT_USER password

Same values as `airflow/.env` — this is GitHub's equivalent of that file.

## 6.3 Lint config — `.sqlfluff` (repo root)

SQLFluff = a linter for SQL: enforces consistent casing, indentation, and catches real mistakes (unqualified ambiguous columns). Minimal config:

```ini
[sqlfluff]
dialect = snowflake
templater = jinja
exclude_rules = ambiguous.column_count, structure.column_order

[sqlfluff:indentation]
tab_space_size = 4
```

Run it locally first — `pip install sqlfluff`, then `sqlfluff lint dbt/models` — and fix (or `sqlfluff fix dbt/models`) *before* wiring it into CI, so your first CI run isn't a wall of red.

## 6.4 CI workflow — `.github/workflows/ci.yml`

```yaml
name: CI
on:
  pull_request:
    branches: [main]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: "3.12"}
      - run: pip install sqlfluff
      - run: sqlfluff lint dbt/models

  build:
    runs-on: ubuntu-latest
    env:
      SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
      DBT_SNOWFLAKE_PASSWORD: ${{ secrets.DBT_SNOWFLAKE_PASSWORD }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: "3.12"}
      - run: pip install dbt-snowflake
      - run: dbt deps --project-dir dbt
      - run: dbt build --project-dir dbt --profiles-dir airflow/include --target ci
```

Every PR now builds every model and runs every test — grain tests, SCD2 no-overlap, relationships, all of it — in the `ci` schemas before merge is possible.

## 6.5 CD workflow — `.github/workflows/deploy.yml`

```yaml
name: Deploy
on:
  push:
    branches: [main]

permissions:
  contents: write

jobs:
  deploy:
    runs-on: ubuntu-latest
    env:
      SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
      DBT_SNOWFLAKE_PASSWORD: ${{ secrets.DBT_SNOWFLAKE_PASSWORD }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: "3.12"}
      - run: pip install dbt-snowflake
      - run: dbt deps --project-dir dbt
      - run: dbt build --project-dir dbt --profiles-dir airflow/include --target prod
      - run: dbt docs generate --project-dir dbt --profiles-dir airflow/include --target prod
      - name: Publish dbt docs to GitHub Pages
        uses: peaceiris/actions-gh-pages@v4
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          publish_dir: dbt/target
```

- [ ] Repo → Settings → Pages → Source: `gh-pages` branch. After the first merge, your full lineage graph is live at `https://bananya007.github.io/chicago-airbnb-pipeline/` — put the link in the README and on your resume's project line.
- [ ] 🔀 **Commit + raise PR #1** (`feature/ci`): `.sqlfluff` + both workflows + the profiles targets. This PR is special — **it runs its own checks**: the moment you open it, `lint` and `build` execute on the PR page. Watch them go green before merging; if red, fix commits rerun them automatically. (Do 6.6's branch protection *after* this merges, since the checks must exist once before GitHub can require them.)

## 6.6 Branch protection — make the gate real

Repo → Settings → Branches → Add rule for `main`: ✅ Require a pull request before merging, ✅ Require status checks to pass (select `lint` and `build`). Now a red CI physically blocks the merge button — even for you.

⚠️ **Break it on purpose:** open a PR that violates a test (the trusty duplicate row… or simpler, a model edit that breaks `unique`). Watch CI go red and the merge button lock. Screenshot for the README. Fix with a second commit, watch it unlock. That screenshot pair is your CI story.

## 6.7 Exposures — lineage all the way to the dashboards

`dbt/models/marts/_exposures.yml`:

```yaml
version: 2

exposures:
  - name: pricing_market_dashboard
    label: "Pricing & Market (Q1-3, Q9)"
    type: dashboard
    url: <tableau public url>
    depends_on: [ref('fct_calendar_day'), ref('pricing_gap'), ref('dim_listing'), ref('dim_date')]
    owner: {name: "Gayathri Ananya", email: "<you>"}
  - name: occupancy_hosts_dashboard
    label: "Occupancy, Revenue & Hosts (Q4-8)"
    type: dashboard
    url: <tableau public url>
    depends_on: [ref('est_occupancy'), ref('dim_host'), ref('dim_listing')]
    owner: {name: "Gayathri Ananya", email: "<you>"}
```

The docs site now draws lineage **source → raw → staging → marts → dashboard**. One screenshot of that graph says more than a page of prose.

## 6.8 The two dashboards

**Tooling reality (state it, don't hide it):** Tableau Public cannot connect to Snowflake. As a student you get **Tableau Desktop free for a year** ([tableau.com/academic/students](https://www.tableau.com/academic/students) with your hawk.illinoistech.edu email) — Desktop connects to Snowflake directly for building. Publishing to Tableau Public bakes the data in as a **static extract**, re-published manually each quarter (add that step to the runbook). Say exactly this in the README; implying a live connection would be the kind of overselling this project exists to avoid.

- **Dashboard 1 — Pricing & Market** (Q1–3, Q9): price by month × neighbourhood × room type; pricing-gap distribution with the ≥5-peer rule noted; min-nights vs price positioning; QoQ listing entries/exits.
- **Dashboard 2 — Occupancy, Revenue & Hosts** (Q4–8): estimated occupancy by neighbourhood/type; forward-availability by month; est. revenue segments; superhost/response-rate vs occupancy; host-quality changes from SCD2.
- Each chart's subtitle = its numbered question, verbatim. Estimates labeled "estimated (San Francisco Model, 50% review rate)" on the face of the chart, with the sensitivity note.

## 6.9 Final README pass, then — and only then — the resume bullets

- [ ] README: problem statement + 9 questions, architecture diagram, both dashboard screenshots + links, dbt docs link, CI badge (`![CI](https://github.com/bananya007/chicago-airbnb-pipeline/actions/workflows/ci.yml/badge.svg)`), the failure-email + red-CI screenshots, "how a quarterly snapshot flows through" narrative, Inside Airbnb attribution, and the honesty notes (estimates, static extracts, Airflow-is-deliberate-overkill).
- [ ] Resume bullets: write them now, from what exists, per the rule at the top of this playbook. Bring them back to this doc's author for a final pass against the repo.
- [ ] 🔀 **Commit + raise PR #2** (`feature/dashboards`): exposures, README final pass, screenshots. This merge is the one that publishes your final dbt docs with dashboard lineage. Merge — and that's the project.

**✅ Phase 6 acceptance criteria**
- A PR with a deliberately broken test is *blocked* by CI; fixed, it merges and auto-deploys.
- dbt docs URL is public and shows source→dashboard lineage including exposures.
- Both dashboards live on Tableau Public, questions as subtitles, estimates labeled.
- README final; resume bullets drafted from the built system.

**🎤 What you can now say in interviews:**
- "Nothing reaches main unbuilt: every PR lints and runs the full dbt build — all tests, grain checks, SCD2 validation — in a throwaway CI schema, and branch protection blocks merge on red. Merges auto-deploy to prod and republish lineage docs."
- "The lineage is public — one URL from raw S3 files to each dashboard tile, via dbt exposures."
- "At 500 models I'd switch CI to state-comparison so PRs only build what changed — at my 15, full builds in minutes are the simpler, correct choice."
- "Dashboard data is a static extract republished quarterly — Tableau Public can't hold a live Snowflake connection, and the README says so plainly."

**Deliverables:** `.sqlfluff`, 2 workflows, branch protection, secrets; public dbt docs URL; exposures; 2 Tableau Public dashboards; final README with badges + screenshots; resume bullets; PRs merged.

---

*Living document — all phases finalized. Update it as you build: note what broke, what surprised you, and how long things took — those notes are your interview stories. When Phase 6 is done, bring the drafted resume bullets back for a final pass against the repo.*
