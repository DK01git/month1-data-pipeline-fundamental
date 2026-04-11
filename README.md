# Retail Data Ingestion Pipeline

## Overview

End-to-end daily data ingestion pipeline for a retail POS system.
Ingests pipe-delimited CSV exports from MinIO object storage,
applies business transformations, and loads aggregated fact data
to PostgreSQL for reporting.

```
MinIO (raw CSV files)
    → PostgreSQL staging layer  (raw copy)
    → PostgreSQL curated layer  (transformed + aggregated)
    → Slack notifications       (success / failure alerts)
```

**Verified output (10,050 source rows):**

- 50 invalid rows captured in `staging.error_log`
- 10,000 valid rows aggregated to 6,361 store-product-date rows
- Revenue across 5 categories, 50 stores, 200 products

---

## Repository Structure

```
retail-data-pipeline/
├── README.md                        ← you are here
├── docker-compose.yml               ← full stack definition
├── .gitignore
│
├── dags/
│   └── retail_daily_ingestion.py   ← Airflow DAG (operational)
│
├── transforms/
│   ├── filter_transform.py         ← Transform 1: invalid row removal
│   ├── derived_transform.py        ← Transform 2: derived columns
│   └── aggregate_transform.py      ← Transform 3: daily aggregation
│
├── scripts/
│   ├── generate_realistic_data.py  ← generates 10,050 rows + uploads to MinIO
│   └── generate_sample_data.py     ← generates 100 rows (quick test)
│
├── init-db/
│   └── 01_schema.sql               ← auto-creates schemas + tables on first boot
│
└── docs/
    ├── diluksha-perera-month1-pipeline-design.md
    ├── diluksha-perera-month1-airflow-dag.py
    ├── diluksha-perera-month1-transforms.md
    ├── diluksha-perera-month1-error-handling.md
    └── diluksha-perera-month1-monitoring-setup.md
```

---

## IMPORTANT — DAG File Note

There are two copies of the DAG file in this repository:

| File                                         | Purpose                                                |
| -------------------------------------------- | ------------------------------------------------------ |
| `dags/retail_daily_ingestion.py`             | **USE THIS ONE** — operational file that Airflow reads |
| `docs/diluksha-perera-month1-airflow-dag.py` | Submission document only — do not move to dags/        |

**Airflow requires the DAG file to be in the `dags/` folder.**
The `dag_id` inside the file (`retail_daily_ingestion`) must match
what Airflow expects. The submission copy in `docs/` is for
evaluation purposes only — running it from `docs/` will cause
a duplicate DAG error in Airflow.

---

## Prerequisites

| Tool           | Version       | Purpose                        |
| -------------- | ------------- | ------------------------------ |
| Docker Desktop | Latest        | Runs all services              |
| Python         | 3.10.11       | Data generation scripts        |
| pip packages   | boto3, pandas | MinIO upload + data generation |

---

## Quick Start

### Step 1 — Clone the repository

```bash
git clone https://github.com/yourusername/retail-data-pipeline.git
cd retail-data-pipeline
```

### Step 2 — Install Python dependencies

```bash
pip install boto3 pandas
```

### Step 3 — Start the Docker stack

```bash
# First time only — initialise Airflow database and admin user
docker-compose up airflow-init

# Start all services
docker-compose up -d
```

Wait 60 seconds then verify all 5 containers are healthy:

```bash
docker-compose ps
```

Expected:

```
NAME                         STATUS
month1-airflow-scheduler-1   Up (running)
month1-airflow-webserver-1   Up (healthy)
month1-minio-1               Up (healthy)
month1-postgres-meta-1       Up (healthy)
month1-postgres-retail-1     Up (healthy)
```

### Step 4 — Verify postgres-retail network connection

```bash
docker exec -it month1-airflow-webserver-1 bash -c "nc -zv postgres-retail 5432"
```

Expected: `Connection to postgres-retail succeeded`

If it fails:

```bash
docker network connect --alias postgres-retail month1_default month1-postgres-retail-1
```

---

## Configure Airflow

Open http://localhost:8080

```
Username: airflow
Password: airflow
```

### Connections (Admin → Connections → "+")

**minio_s3:**

```
Connection Id:         minio_s3
Connection Type:       Amazon Web Services
AWS Access Key ID:     minioadmin
AWS Secret Access Key: minioadmin
Extra:                 {"endpoint_url": "http://minio:9000"}
```

**postgres_retail:**

```
Connection Id:   postgres_retail
Connection Type: Postgres
Host:            postgres-retail
Database:        retailco
Login:           retailco
Password:        retailco
Port:            5432
```

**slack_webhook:**

```
Connection Id:   slack_webhook
Connection Type: HTTP
Host:            https://hooks.slack.com/services
Password:        /YOUR/SLACK/WEBHOOK/PATH
```

Note: Replace Password with your own Slack webhook path.
Create one at https://api.slack.com/apps

### Variable (Admin → Variables → "+")

```
Key:   source_bucket
Value: raw-data
```

---

## Run the Pipeline

### Step 1 — Generate and upload sample data

```bash
python scripts/generate_realistic_data.py
```

Note the date printed in the output — for example:

```
Generating realistic data for: 2026-04-07
✔ Uploaded: retail-data/2026-04-07/store_locations.csv     (50 rows)
✔ Uploaded: retail-data/2026-04-07/product_catalog.csv    (200 rows)
✔ Uploaded: retail-data/2026-04-07/sales_transactions.csv (10,050 rows)
```

### Step 2 — Trigger the DAG

```bash
docker exec -it month1-airflow-webserver-1 \
  airflow dags trigger retail_daily_ingestion
```

Check the `data_interval_start` in the output. It must match
the date from Step 1. If it doesn't match, see Troubleshooting.

### Step 3 — Monitor in Airflow UI

Open http://localhost:8080 → DAGs → retail_daily_ingestion → Graph tab

Expected task sequence:

```
check_files_exist    → green
branch_on_files      → green
alert_missing_files  → skipped (pink)
ingest_to_staging    → green
transform_sales      → green
load_curated         → green
archive_source_files → green
send_notification    → green
```

Check Slack #pipeline-alerts for SUCCESS notification.

---

## Verify Results

```bash
docker exec -it month1-postgres-retail-1 \
  psql -U retailco -d retailco -c "
SELECT 'staging.sales_transactions', COUNT(*) FROM staging.sales_transactions
UNION ALL
SELECT 'staging.error_log',          COUNT(*) FROM staging.error_log
UNION ALL
SELECT 'staging.product_catalog',    COUNT(*) FROM staging.product_catalog
UNION ALL
SELECT 'staging.store_locations',    COUNT(*) FROM staging.store_locations
UNION ALL
SELECT 'curated.fact_daily_sales',   COUNT(*) FROM curated.fact_daily_sales;"
```

Expected:

```
staging.sales_transactions | 10050
staging.error_log           |    50
staging.product_catalog     |   200
staging.store_locations     |    50
curated.fact_daily_sales    |  6361
```

```bash
# Revenue by category
docker exec -it month1-postgres-retail-1 \
  psql -U retailco -d retailco -c "
SELECT category,
       COUNT(DISTINCT store_id)  AS stores,
       SUM(total_quantity)       AS units_sold,
       ROUND(SUM(total_revenue)::NUMERIC, 2) AS revenue
FROM curated.fact_daily_sales
GROUP BY category
ORDER BY revenue DESC;"
```

## Pipeline Architecture

### Three-Layer Design

| Layer   | Storage                     | Load Pattern      | Retention            |
| ------- | --------------------------- | ----------------- | -------------------- |
| Raw     | MinIO `raw-data` bucket     | Never modified    | 30 days then archive |
| Staging | PostgreSQL `staging` schema | TRUNCATE + INSERT | Current run only     |
| Curated | PostgreSQL `curated` schema | UPSERT            | Permanent history    |

### Task Flow

```
check_files_exist       S3KeySensor — waits up to 30 min for files
        |
branch_on_files         BranchPythonOperator — verifies all 3 files
        |                        |
ingest_to_staging        alert_missing_files  ← dead-letter path
        |
transform_sales         4 transforms: filter → derive → aggregate → enrich
        |
load_curated            PostgresOperator UPSERT into curated.fact_daily_sales
        |
archive_source_files    Copies CSVs to archive-data bucket
        |
send_notification       Slack alert — always runs via trigger_rule=ALL_DONE
```

### Transformations

| Step      | Operation                                        | Input → Output                         |
| --------- | ------------------------------------------------ | -------------------------------------- |
| Filter    | Remove invalid rows                              | 10,050 → 10,000 rows + 50 in error_log |
| Derived   | Calculate line_total, tax_amount, total_with_tax | Adds 5 columns                         |
| Aggregate | Group by store + product + date                  | 10,000 → 6,361 rows                    |
| Enrich    | Join with product catalog                        | Adds category, subcategory, brand      |

### Error Handling

| Mechanism     | Implementation                                     |
| ------------- | -------------------------------------------------- |
| Missing files | BranchPythonOperator routes to alert_missing_files |
| Invalid rows  | Captured in staging.error_log with full JSON       |
| Row anomaly   | AirflowException if count < 100                    |
| Task failure  | Slack alert via on_failure_callback                |
| Retries       | 3 retries × 30s on ingestion tasks                 |
| SLA           | timedelta(hours=2) on all tasks                    |
| Timeout       | dagrun_timeout=4 hours                             |

---

## Troubleshooting

### Sensor times out (check_files_exist fails after 30 min)

Check what date the sensor is looking for:

- Airflow UI → click check_files_exist → Logs
- Find line: `Poking for key: s3://raw-data/retail-data/YYYY-MM-DD/`
- Note the date

Edit `scripts/generate_realistic_data.py`:

```python
RUN_DATE = "YYYY-MM-DD"  # use the date from sensor log
```

Re-run:

```bash
python scripts/generate_realistic_data.py
```

### postgres-retail not reachable from Airflow

```bash
docker network disconnect month1_default month1-postgres-retail-1
docker network connect --alias postgres-retail \
  month1_default month1-postgres-retail-1
docker exec -it month1-airflow-webserver-1 \
  bash -c "nc -zv postgres-retail 5432"
```

### Port 5432 conflict with existing PostgreSQL

```bash
# Find what's using port 5432
netstat -ano | findstr :5432

# Stop conflicting container if it's Docker
docker stop <container-name>

# Restart postgres-retail
docker-compose up -d postgres-retail
```

### Airflow login fails (Invalid credentials)

```bash
docker exec -it month1-airflow-webserver-1 \
  airflow users create \
  --username airflow --password airflow \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com
```

### DAG shows import error

```bash
docker exec -it month1-airflow-webserver-1 \
  bash -c "airflow dags list-import-errors"
```

Common causes:

- Provider package not installed — check `_PIP_ADDITIONAL_REQUIREMENTS` in docker-compose.yml
- Syntax error in DAG — check `dags/retail_daily_ingestion.py`

### data_interval_start doesn't match uploaded data date

This is expected Airflow behaviour. The pipeline processes the
previous day's data interval, not today's date.

Solution: Always run `generate_realistic_data.py` first, note the
date it prints, then trigger the DAG. If dates still don't match,
hardcode `RUN_DATE` in the script to match `data_interval_start`.

---

## Documentation

| Document                                          | Description                                     |
| ------------------------------------------------- | ----------------------------------------------- |
| `docs/diluksha-perera-month1-pipeline-design.md`  | Full architecture, layers, task dependencies    |
| `docs/diluksha-perera-month1-transforms.md`       | All 4 transforms with verified row counts       |
| `docs/diluksha-perera-month1-error-handling.md`   | Retry policies, callbacks, dead-letter patterns |
| `docs/diluksha-perera-month1-monitoring-setup.md` | Airflow UI monitoring, Slack alerts             |
| `docs/diluksha-perera-month1-airflow-dag.py`      | Submission copy of DAG (do not run from here)   |

---

## Service Credentials

| Service       | URL                   | Username   | Password   |
| ------------- | --------------------- | ---------- | ---------- |
| Airflow UI    | http://localhost:8080 | airflow    | airflow    |
| MinIO Console | http://localhost:9001 | minioadmin | minioadmin |
| PostgreSQL    | localhost:5432        | retailco   | retailco   |
