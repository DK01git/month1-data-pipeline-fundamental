# Pipeline Architecture: Retail Data Ingestion
**Author:** Diluksha Perera
**Date:** 2026-04-08
**DAG ID:** `retail_daily_ingestion`
**Submission:** diluksha-perera-month1-pipeline-design.md

---

## 1. Overview

**DAG Name:** `retail_daily_ingestion`
**Purpose:** Ingest daily retail POS CSV exports from MinIO object
storage into PostgreSQL, applying business transformations and
loading aggregated fact data to a curated layer for reporting.

**Source-to-Target Mapping:**

| Source File | Staging Table | Curated Table |
|---|---|---|
| sales_transactions.csv | staging.sales_transactions | curated.fact_daily_sales |
|      |

---

## 2. Architecture Layers

### Raw / Landing Layer — MinIO Bucket
- **Location:** `s3://raw-data/retail-data/{YYYY-MM-DD}/`
- **Role:** Immutable source of truth. Files are never modified.
- **Files:** `sales_transactions.csv`, `product_catalog.csv`, `store_locations.csv`
- **Format:** CSV, pipe-delimited (`|`), with headers
- **Volume:** 10,050 rows/day sales (10,000 valid + 50 invalid), 200 products, 50 stores
- **Retention:** Copied to `s3://archive-data/` after successful load

### Staging Layer — PostgreSQL (`staging` schema)
- **Role:** Raw copy of source data. No business transformations.
  Enables reprocessing without re-downloading from MinIO.
- **Tables:**
  - `staging.sales_transactions` — raw sales rows including invalid
  - `staging.product_catalog` — raw product reference data
  - `staging.store_locations` — raw store reference data
  - `staging.fact_daily_sales_stage` — intermediate enriched data for PostgresOperator UPSERT
  - `staging.error_log` — invalid rows captured during transform filter
- **Load pattern:** TRUNCATE + INSERT (full reload per run — idempotent)

### Curated Layer — PostgreSQL (`curated` schema)
- **Role:** Transformed, aggregated, business-ready data for reporting.
- **Tables:** `curated.fact_daily_sales`
- **Load pattern:** UPSERT on PRIMARY KEY `(store_id, product_id, transaction_date)`
- **Verified output:** 6,361 aggregated rows from 10,000 valid transactions

---

## 3. Pipeline Flow

```
[MinIO: s3://raw-data/retail-data/{date}/]
              |
              v
[S3KeySensor: check_files_exist]
  Waits up to 30 min, pokes every 30s
  Confirms sales_transactions.csv exists
  bucket_key uses data_interval_start for accurate date resolution
              |
              v
[BranchPythonOperator: branch_on_files]
  Checks ALL 3 files exist via S3Hook.check_for_key()
  Returns task_id of next task to execute
              |                       |
     all 3 present              any file missing
              |                       |
              v                       v
[ingest_to_staging]         [alert_missing_files]
  TRUNCATE + INSERT           Raises Exception
  all 3 staging tables        logs missing paths
  pipe-delimited CSV          routes to notification
  row count anomaly check
  (raises AirflowException
   if count < 100)
              |                       |
              v                       |
[transform_sales]                     |
  1. Filter invalid rows              |
     -> write to staging.error_log    |
  2. Derived columns                  |
     line_total, tax_amount           |
  3. Aggregate by                     |
     store/product/date               |
  4. Enrich with product catalog      |
  -> writes to                        |
     staging.fact_daily_sales_stage   |
              |                       |
              v                       |
[load_curated - PostgresOperator]     |
  UPSERT from                         |
  staging.fact_daily_sales_stage      |
  into curated.fact_daily_sales       |
  ON CONFLICT DO UPDATE               |
              |                       |
              v                       |
[archive_source_files]                |
  Copy CSVs to archive-data bucket    |
              |                       |
              +----------+-----------+
                         |
                         v
               [send_notification]
               trigger_rule=ALL_DONE
               Posts SUCCESS or FAILED
               to Slack #pipeline-alerts
               with failed task list
```

---

## 4. Task Dependency Map

### Happy Path (all files present):
```
check_files_exist >> branch_on_files >> ingest_to_staging
>> transform_sales >> load_curated >> archive_source_files
>> send_notification
```

### Dead-Letter Path (missing files):
```
check_files_exist >> branch_on_files >> alert_missing_files
>> send_notification
```

### Task Trigger Rules:

| Task | trigger_rule | Reason |
|---|---|---|
| check_files_exist | all_success (default) | Must succeed before branching |
| branch_on_files | all_success | Only branch if sensor passed |
| ingest_to_staging | all_success | Only ingest if files confirmed |
| transform_sales | all_success | Only transform if ingest succeeded |
| load_curated | all_success | Only load if transform succeeded |
| archive_source_files | all_success | Only archive if load succeeded |
| send_notification | **all_done** | Always notify — success or failure |

### Concurrency Control:
- `max_active_runs=1` — prevents concurrent DAG runs from
  truncating staging while a previous run's transform is reading it

### Airflow Date Model:
- `schedule_interval='0 6 * * *'` — daily at 06:00 UTC
- `data_interval_start` used for file path resolution — not `ds`
- `catchup=False` — only most recent incomplete interval runs
- `start_date=datetime(2026, 4, 5)` — produces `data_interval_start=2026-04-06`

---

## 5. Error Handling Strategy

### Retry Policies Per Task:

| Task | Retries | Retry Delay | Rationale |
|---|---|---|---|
| check_files_exist | 5 (sensor poke) | 30s poke interval | Files may arrive late from POS |
| ingest_to_staging | 3 | 30s | Transient MinIO/PostgreSQL issues |
| transform_sales | 1 | 30s | Logic errors need manual investigation |
| load_curated | 3 | 30s | Transient DB connection issues |
| archive_source_files | 2 | 30s | Non-critical, should not block pipeline |
| send_notification | 0 | N/A | Notify once only |

### Failure Callbacks:
- All tasks: `on_failure_callback=send_failure_alert`
- Posts to Slack `#pipeline-alerts` with DAG, task, run date, exception
- DAG level: `on_failure_callback=send_failure_alert`
- DAG level: `on_success_callback=dag_success_callback`
- SLA: `sla=timedelta(hours=2)` in default_args — all tasks

### Dead-Letter Handling:
- Invalid rows: written to `staging.error_log` with source_table,
  raw_data (JSON), error_message, logged_at
- Verified: 50 invalid rows captured per run in production test
- Row count anomaly: raises `AirflowException` if staging row count < 100
- Missing files: `alert_missing_files` task raises Exception, routes to notification
- DAG timeout: `dagrun_timeout=timedelta(hours=4)`

---

## Infrastructure

| Service | Image | Purpose | Internal Host | Host Port |
|---|---|---|---|---|
| Apache Airflow | apache/airflow:2.9.0-python3.11 | Orchestration | — | 8080 |
| PostgreSQL (retail) | postgres:16-alpine | Staging + Curated | postgres-retail:5432 | 5432 |
| PostgreSQL (meta) | postgres:16-alpine | Airflow metadata | postgres-meta:5432 | — |
| MinIO | minio/minio:latest | Source data store | minio:9000 | 9001 |

### Airflow Connections Configured:
| Connection ID | Type | Purpose |
|---|---|---|
| minio_s3 | Amazon Web Services | MinIO S3-compatible storage |
| postgres_retail | Postgres | Retail staging + curated DB |
| slack_webhook | HTTP | Slack failure/success alerts |

### Airflow Variables Configured:
| Key | Value | Purpose |
|---|---|---|
| source_bucket | raw-data | MinIO bucket name for source files |
