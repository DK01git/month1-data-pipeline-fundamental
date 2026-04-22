# =============================================================
# FILE: retail_daily_ingestion.py
# DELIVERABLE: diluksha-perera-month1-airflow-dag.py
# Challenge File, Part 2: "Build the Airflow DAG"
# Session File, Learning Objectives: "Design and build Apache
#   Airflow DAGs with multiple operators and task dependencies"
# =============================================================

from __future__ import annotations

import io
import json
import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.http.hooks.http import HttpHook
import great_expectations as gx
from great_expectations.core.batch import BatchRequest

log = logging.getLogger(__name__)

# =============================================================
# DEFAULT ARGS
# Challenge File, Part 4: "default_args" block
# =============================================================
default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "email": ["diluksha@bistcglobal.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    # Challenge File, Part 4: "sla=timedelta(hours=2) on tasks"
    "sla": timedelta(hours=2),
}

# =============================================================
# CONSTANTS
# Challenge File, Part 2: "Airflow Variable: source_bucket"
# Using Variable.get so it reads from Airflow UI — not hardcoded
# Challenge File, Tips: "Avoid hardcoding connection strings"
# =============================================================
MINIO_CONN_ID    = "minio_s3"
POSTGRES_CONN_ID = "postgres_retail"
RUN_DATE_MACRO   = "{{ ds }}"  # Airflow execution date YYYY-MM-DD

# These are the 3 source files from the challenge
# Challenge File, Data Profile: "sales_transactions.csv,
#   product_catalog.csv, store_locations.csv"
SOURCE_FILES = [
    "sales_transactions.csv",
    "product_catalog.csv",
    "store_locations.csv",
]


# =============================================================
# HELPER: failure alert callback
# Challenge File, Part 4: "on_failure_callback → send_failure_alert()"
# =============================================================
def send_failure_alert(context):
    """
    Posts failure alert to Slack via webhook connection.
    Challenge File, Part 4: "on_failure_callback, alerting,
      set up email/Slack notifications on task failure"
    Connection: slack_webhook (Admin → Connections in Airflow UI)
    Secrets stored in Airflow connection — not hardcoded.
    Challenge File, Common Mistakes: "Hardcoding connection strings"
    """
    dag_id   = context["dag"].dag_id
    task_id  = context["task_instance"].task_id
    run_date = context.get("ds", "unknown")
    exception= str(context.get("exception", "No exception details"))

    # Build Slack message block
    message = {
        "text": f":red_circle: *Pipeline Failure Alert*",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": ":red_circle: Pipeline Failure Alert"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:*\n{dag_id}"},
                    {"type": "mrkdwn", "text": f"*Task:*\n{task_id}"},
                    {"type": "mrkdwn", "text": f"*Run Date:*\n{run_date}"},
                    {"type": "mrkdwn", "text": f"*Status:*\nFailed"},
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Exception:*\n```{exception[:500]}```"
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "Check Airflow logs for full traceback."
                    }
                ]
            }
        ]
    }

    try:
        # Webhook path stored in Airflow connection Password field
        # Host: https://hooks.slack.com/services
        # Password: /TXXXXXXX/BXXXXXXX/XXXXXXXXXX (path only)
        http_hook = HttpHook(
            method="POST",
            http_conn_id="slack_webhook"
        )
        # Combine host + password to form full webhook URL
        conn = http_hook.get_connection("slack_webhook")
        webhook_path = conn.password
        http_hook.run(
            endpoint=webhook_path,
            data=json.dumps(message),
            headers={"Content-Type": "application/json"},
        )
        log.info("Slack alert sent for task: %s", task_id)
    except Exception as e:
        # Never let notification failure break the pipeline
        log.error("Slack notification failed: %s", str(e))

# =============================================================
# TASK 1 — BRANCH: check all 3 files exist
# Challenge File, Part 2: "branch_on_files | BranchPythonOperator
#   | Decide whether all files arrived"
# WHY: S3KeySensor checks ONE file. We need ALL 3 confirmed
#   before ingestion. Missing file = broken joins downstream.
# =============================================================
def branch_on_files(**context) -> str:
    """
    Check all 3 source files exist in MinIO for this run date.
    Returns task_id to execute next — either ingest or alert.
    """
    source_bucket = Variable.get("source_bucket", default_var="raw-data")
    run_date = context["data_interval_start"].strftime("%Y-%m-%d")  # YYYY-MM-DD from Airflow
    s3_hook       = S3Hook(aws_conn_id=MINIO_CONN_ID)

    missing = []
    for fname in SOURCE_FILES:
        key = f"retail-data/{run_date}/{fname}"
        if not s3_hook.check_for_key(key=key, bucket_name=source_bucket):
            missing.append(key)
            log.warning("Missing file: s3://%s/%s", source_bucket, key)

    if missing:
        log.error("Branch → alert_missing_files. Missing: %s", missing)
        return "alert_missing_files"

    log.info("Branch → ingest_to_staging. All 3 files confirmed.")
    return "ingest_to_staging"


# =============================================================
# TASK 2 — ALERT: missing files dead-letter path
# Challenge File, Sample Diagram: "missing_files → alert_and_skip"
# =============================================================
def alert_missing_files(**context):
    run_date = context["data_interval_start"].strftime("%Y-%m-%d")
    raise Exception(
        f"Pipeline aborted: one or more source files missing "
        f"for run_date={run_date}. Check MinIO bucket."
    )


# =============================================================
# TASK 3 — INGEST: download CSVs from MinIO → staging tables
# Challenge File, Part 2: "ingest_to_staging | PythonOperator
#   | Download CSV from MinIO, load to PostgreSQL staging"
# Challenge File, Ingestion Task code block
# =============================================================
def ingest_to_staging(**context):
    """
    Downloads all 3 pipe-delimited CSVs from MinIO and loads
    them into staging tables. Uses TRUNCATE + INSERT for
    idempotency — safe to rerun without duplicating data.
    Challenge File Tips: "Think about idempotency"
    """
    source_bucket = Variable.get("source_bucket", default_var="raw-data")
    run_date = context["data_interval_start"].strftime("%Y-%m-%d")
    s3_hook       = S3Hook(aws_conn_id=MINIO_CONN_ID)
    pg_hook       = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Map: source file → staging table + expected columns
    # Challenge File, Source Schema section — all 3 schemas
    ingestion_map = {
        "sales_transactions.csv": {
            "table":   "staging.sales_transactions",
            "columns": [
                "transaction_id", "store_id", "product_id",
                "quantity", "unit_price", "discount_pct",
                "transaction_date", "payment_method", "customer_id",
            ],
        },
        "product_catalog.csv": {
            "table":   "staging.product_catalog",
            "columns": [
                "product_id", "product_name", "category",
                "subcategory", "brand", "cost_price",
                "list_price", "supplier_id",
            ],
        },
        "store_locations.csv": {
            "table":   "staging.store_locations",
            "columns": [
                "store_id", "store_name", "city", "state",
                "region", "store_type", "opening_date",
            ],
        },
    }

    for fname, config in ingestion_map.items():
        key = f"retail-data/{run_date}/{fname}"
        log.info("Reading s3://%s/%s", source_bucket, key)

        # Read pipe-delimited CSV from MinIO
        # Challenge File: "Format: CSV with headers, pipe-delimited"
        file_content = s3_hook.read_key(
            key=key,
            bucket_name=source_bucket,
        )
        df = pd.read_csv(io.StringIO(file_content), delimiter="|")

        # Validate row count — catch empty files early
        if df.empty:
            raise Exception(f"File {fname} loaded 0 rows — aborting.")

        log.info("File %s loaded: %d rows", fname, len(df))

        # Replace NaN with None so PostgreSQL gets NULL not "nan"
        # Challenge File: "customer_id (string, nullable)"
        df = df.where(pd.notna(df), None)

        # TRUNCATE then INSERT — idempotency pattern
        # Challenge File Tips: "DAGs should be safe to rerun
        #   without side effects"
        pg_hook.run(f"TRUNCATE TABLE {config['table']};")
        pg_hook.insert_rows(
            table=config["table"],
            rows=df[config["columns"]].values.tolist(),
            target_fields=config["columns"],
        )
        log.info("Staged %d rows → %s", len(df), config["table"])

    # Row count anomaly check
    # Challenge File, Part 4: "If count < 100 raise AirflowException"
    count = pg_hook.get_first(
        "SELECT COUNT(*) FROM staging.sales_transactions;"
    )[0]
    log.info("staging.sales_transactions row count: %d", count)
    if count < 100:
        raise Exception(
            f"Anomaly: only {count} rows in staging.sales_transactions. "
            f"Expected ≥100. Investigate source file."
        )


# =============================================================
# TASK 4 — TRANSFORM: business logic on staging data
# Challenge File, Part 3: "transform_sales task"
# Challenge File, Transformation Examples — all 4 transforms
# =============================================================
def transform_sales(**context):
    """
    Reads from staging, applies 4 transforms, writes to
    curated.fact_daily_sales via UPSERT.

    Transforms applied (Challenge File, Part 3):
      1. Filter  — remove nulls and invalid records
      2. Derived — line_total, tax_amount, total_with_tax
      3. Aggregate — daily summary by store + product
      4. Lookup  — enrich with product category from catalog
    """
    run_date = context["data_interval_start"].strftime("%Y-%m-%d")
    pg_hook  = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # --- Read staging data ---
    df = pg_hook.get_pandas_df(
        "SELECT * FROM staging.sales_transactions;"
    )
    log.info("Transform input: %d rows from staging", len(df))

    # --- Transform 1: Filter ---
    # Challenge File, Part 3: "Filter Transform"
    before = len(df)
    # Identify bad rows BEFORE filtering — capture them for error_log
    bad_mask = ~(
        df["transaction_id"].notna() &
        (df["quantity"].astype(float) > 0) &
        (df["unit_price"].astype(float) > 0)
    )
    bad_rows = df[bad_mask].copy()

    # Write bad rows to staging.error_log
    if not bad_rows.empty:
        error_rows = []
        for _, bad_row in bad_rows.iterrows():
            error_rows.append((
                "staging.sales_transactions",          # source_table
                bad_row.to_json(),                     # raw_data as JSON
                "Failed filter: null transaction_id "
                "or quantity/unit_price <= 0",         # error_message
            ))
        pg_hook.insert_rows(
            table="staging.error_log",
            rows=error_rows,
            target_fields=["source_table", "raw_data", "error_message"],
        )
        log.warning(
            "Wrote %d invalid rows to staging.error_log",
            len(bad_rows)
        )

    # Now filter — keep only valid rows
    df = df[~bad_mask].copy()
    removed = before - len(df)
    if removed > 0:
        log.warning("Filter removed %d invalid rows", removed)

    if df.empty:
        raise Exception("No valid rows after filter — aborting transform.")

    # Cast numeric columns explicitly — CSV may import as strings
    df["quantity"]     = df["quantity"].astype(int)
    df["unit_price"]   = df["unit_price"].astype(float)
    df["discount_pct"] = df["discount_pct"].astype(float)

    # --- Transform 2: Derived Columns ---
    # Challenge File, Part 3: "Derived Column Transform"
    df["line_total"]     = (
        df["quantity"] * df["unit_price"] * (1 - df["discount_pct"] / 100)
    ).round(2)
    df["tax_amount"]     = (df["line_total"] * 0.08).round(2)
    df["total_with_tax"] = (df["line_total"] + df["tax_amount"]).round(2)
    df["load_timestamp"] = pd.Timestamp.now()
    df["source_file"]    = f"sales_transactions_{run_date}.csv"

    # --- Transform 3: Aggregate ---
    # Challenge File, Part 3: "Aggregate Transform"
    daily_summary = df.groupby(
        ["store_id", "product_id", "transaction_date"]
    ).agg(
        total_quantity   =("quantity",     "sum"),
        total_revenue    =("line_total",   "sum"),
        total_tax        =("tax_amount",   "sum"),
        transaction_count=("transaction_id","count"),
        avg_discount     =("discount_pct", "mean"),
    ).reset_index()

    daily_summary["total_revenue"] = daily_summary["total_revenue"].round(2)
    daily_summary["total_tax"]     = daily_summary["total_tax"].round(2)
    daily_summary["avg_discount"]  = daily_summary["avg_discount"].round(2)
    daily_summary["load_timestamp"]= pd.Timestamp.now()

    log.info("Aggregated to %d store-product-date rows", len(daily_summary))

    # --- Transform 4: Lookup / Enrich ---
    # Challenge File, Part 3: "Lookup Transform"
    product_df = pg_hook.get_pandas_df(
        "SELECT product_id, category, subcategory, brand "
        "FROM staging.product_catalog;"
    )
    enriched = daily_summary.merge(product_df, on="product_id", how="left")

    # Warn if any products couldn't be enriched
    unmatched = enriched["category"].isna().sum()
    if unmatched > 0:
        log.warning("%d rows have no matching product in catalog", unmatched)

    log.info("Enriched dataset: %d rows ready for curated load", len(enriched))

    # --- Load to curated via UPSERT ---
    # --- Write enriched data to staging.fact_daily_sales_stage ---
    # PostgresOperator (load_curated) will UPSERT from this table
    # This is the correct pattern: PythonOperator owns pandas,
    # PostgresOperator owns SQL execution
    pg_hook.run("TRUNCATE TABLE staging.fact_daily_sales_stage;")

    rows = []
    for _, row in enriched.iterrows():
        rows.append((
            str(row["store_id"]),
            str(row["product_id"]),
            str(row["transaction_date"]),
            str(row["category"])          if pd.notna(row.get("category"))    else None,
            str(row["subcategory"])       if pd.notna(row.get("subcategory")) else None,
            str(row["brand"])             if pd.notna(row.get("brand"))       else None,
            int(row["total_quantity"]),
            float(row["total_revenue"]),
            float(row["total_tax"]),
            int(row["transaction_count"]),
            float(row["avg_discount"]),
            row["load_timestamp"],
        ))

    pg_hook.insert_rows(
        table="staging.fact_daily_sales_stage",
        rows=rows,
        target_fields=[
            "store_id", "product_id", "transaction_date",
            "category", "subcategory", "brand",
            "total_quantity", "total_revenue", "total_tax",
            "transaction_count", "avg_discount", "load_timestamp",
        ],
    )
    log.info(
        "Wrote %d rows to staging.fact_daily_sales_stage "
        "for PostgresOperator UPSERT", len(rows)
    )


# =============================================================
# TASK 5 — ARCHIVE: move processed files to archive bucket

def archive_source_files(**context):
    """Move processed CSVs from raw-data to archive-data bucket."""
    source_bucket  = Variable.get("source_bucket", default_var="raw-data")
    archive_bucket = "archive-data"
    run_date = context["data_interval_start"].strftime("%Y-%m-%d")
    s3_hook        = S3Hook(aws_conn_id=MINIO_CONN_ID)

    # Create archive bucket if it doesn't exist
    try:
        s3_hook.get_conn().create_bucket(Bucket=archive_bucket)
        log.info("Created archive bucket: %s", archive_bucket)
    except Exception as e:
        log.info("Archive bucket note: %s", str(e))

    for fname in SOURCE_FILES:
        source_key  = f"retail-data/{run_date}/{fname}"
        archive_key = f"retail-data/{run_date}/{fname}"

        s3_hook.copy_object(
            source_bucket_key=source_key,
            dest_bucket_key=archive_key,
            source_bucket_name=source_bucket,
            dest_bucket_name=archive_bucket,
        )
    for fname in SOURCE_FILES:
        source_key  = f"retail-data/{run_date}/{fname}"
        archive_key = f"retail-data/{run_date}/{fname}"

        # Copy to archive bucket
        s3_hook.copy_object(
            source_bucket_key  = source_key,
            dest_bucket_key    = archive_key,
            source_bucket_name = source_bucket,
            dest_bucket_name   = archive_bucket,
        )
        log.info("Archived: %s -> s3://%s/%s",
                 source_key, archive_bucket, archive_key)

        # Delete from raw-data after successful copy
        # Converts copy to move — raw bucket stays clean
        s3_hook.delete_objects(
            bucket=source_bucket,
            keys=[source_key],
        )
        log.info("Deleted from source: s3://%s/%s",
                 source_bucket, source_key)


        # log.info("Archived: %s → s3://%s/%s",
        #          source_key, archive_bucket, archive_key)


# =============================================================
# GX CHECKPOINT 1 — Validate staging.sales_transactions
# Runs after ingest_to_staging, before transform_sales
# Validates: row count, nulls, formats, payment methods
# =============================================================
def validate_staging(**context):
    """
    Great Expectations validation on staging.sales_transactions.
    Checkpoint 1 of 3 in the data quality framework.
    Catches: wrong schema, missing columns, invalid values,
             format violations from inconsistent POS systems.
    """
    run_date = context["data_interval_start"].strftime("%Y-%m-%d")
    log.info("Running GX validation on staging.sales_transactions")

    try:
        context_gx = gx.get_context(
            context_root_dir="/opt/airflow/great_expectations"
        )

        batch_request = BatchRequest(
            datasource_name="retail_postgres",
            data_connector_name="default_inferred_data_connector_name",
            data_asset_name="staging.sales_transactions",
        )

        validator = context_gx.get_validator(
            batch_request=batch_request,
            expectation_suite_name="staging_sales_suite",
        )

        results = validator.validate()

        # Build summary
        total       = results["statistics"]["evaluated_expectations"]
        successful  = results["statistics"]["successful_expectations"]
        failed      = results["statistics"]["unsuccessful_expectations"]

        log.info(
            "GX staging validation: %d/%d expectations passed",
            successful, total
        )

        if not results["success"]:
            failed_expectations = [
                r["expectation_config"]["expectation_type"]
                for r in results["results"]
                if not r["success"]
            ]
            raise AirflowException(
                f"GX staging validation FAILED on run_date={run_date}. "
                f"Failed expectations: {failed_expectations}. "
                f"Check Data Docs for details."
            )

        log.info("GX staging validation PASSED — all %d expectations met", total)

    except AirflowException:
        raise
    except Exception as e:
        log.error("GX staging validation error: %s", str(e))
        raise AirflowException(f"GX validation failed: {str(e)}")


# =============================================================
# GX CHECKPOINT 2 — Validate curated.fact_daily_sales
# Runs after load_curated, before archive_source_files
# Validates: revenue positive, categories valid, no nulls,
#            revenue > tax, transaction counts valid
# =============================================================
def validate_curated(**context):
    """
    Great Expectations validation on curated.fact_daily_sales.
    Checkpoint 2 of 3 in the data quality framework.
    Catches: transformation errors, aggregation bugs,
             missing enrichment, negative revenue.
    """
    run_date = context["data_interval_start"].strftime("%Y-%m-%d")
    log.info("Running GX validation on curated.fact_daily_sales")

    try:
        context_gx = gx.get_context(
            context_root_dir="/opt/airflow/great_expectations"
        )

        batch_request = BatchRequest(
            datasource_name="retail_postgres",
            data_connector_name="default_inferred_data_connector_name",
            data_asset_name="curated.fact_daily_sales",
        )

        validator = context_gx.get_validator(
            batch_request=batch_request,
            expectation_suite_name="curated_sales_suite",
        )

        results = validator.validate()

        total      = results["statistics"]["evaluated_expectations"]
        successful = results["statistics"]["successful_expectations"]
        failed     = results["statistics"]["unsuccessful_expectations"]

        log.info(
            "GX curated validation: %d/%d expectations passed",
            successful, total
        )

        if not results["success"]:
            failed_expectations = [
                r["expectation_config"]["expectation_type"]
                for r in results["results"]
                if not r["success"]
            ]
            raise AirflowException(
                f"GX curated validation FAILED on run_date={run_date}. "
                f"Failed expectations: {failed_expectations}. "
                f"Check Data Docs for details."
            )

        log.info("GX curated validation PASSED — all %d expectations met", total)

    except AirflowException:
        raise
    except Exception as e:
        log.error("GX curated validation error: %s", str(e))
        raise AirflowException(f"GX validation failed: {str(e)}")














# =============================================================
# TASK 6 — NOTIFICATION: success/failure summary
# Challenge File, Part 2: "send_notification | PythonOperator
#   | Send success/failure notification"
# Challenge File, Part 4: trigger_rule=all_done — always runs
# =============================================================
def send_notification(**context):
    """
    Posts pipeline completion status to Slack.
    Runs regardless of upstream state via trigger_rule=ALL_DONE.
    Challenge File, Part 2: "send_notification | PythonOperator
      | Send success/failure notification"
    """
    dag_run  = context["dag_run"]
    run_date = context["data_interval_start"].strftime("%Y-%m-%d")
    # state    = dag_run.get_state()
    tis = context["dag_run"].get_task_instances()
    failed_tasks = [
        ti.task_id for ti in tis
        if ti.state in ("failed", "upstream_failed")
        and ti.task_id != "send_notification"
    ]
    state = "failed" if failed_tasks else "success"

    # Choose emoji based on pipeline state
    emoji = ":white_check_mark:" if state == "success" else ":red_circle:"

    message = {
        "text": f"{emoji} Pipeline {state.upper()}: retail_daily_ingestion",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} Pipeline {state.upper()}"
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*DAG:*\nretail_daily_ingestion"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Run Date:*\n{run_date}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Status:*\n{state.upper()}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Failed Tasks:*\n{', '.join(failed_tasks) if failed_tasks else 'None'}"
                    },
                ]
            }
        ]
    }

    try:
        http_hook = HttpHook(
            method="POST",
            http_conn_id="slack_webhook"
        )
        conn = http_hook.get_connection("slack_webhook")
        webhook_path = conn.password
        http_hook.run(
            endpoint=webhook_path,
            data=json.dumps(message),
            headers={"Content-Type": "application/json"},
        )
        log.info("Slack notification sent | state: %s", state)
    except Exception as e:
        log.error("Slack notification failed: %s", str(e))


def dag_success_callback(context):
    """
    DAG-level success callback.
    Challenge File, Part 4: "on_success_callback → send_status_notification()"
    Fires when entire DAG run completes successfully.
    """
    log.info(
        "DAG completed successfully | DAG: %s | Run: %s",
        context["dag"].dag_id,
        context["run_id"],
    )


# =============================================================
# DAG DEFINITION
# Challenge File, Part 4: schedule_interval, default_args,
#   catchup=False, dagrun_timeout, tags
# max_active_runs=1 → prevents concurrent runs from
#   truncating staging while transform is reading it
# =============================================================
with DAG(
    dag_id="retail_daily_ingestion",
    default_args=default_args,
    description="Retail POS daily ingestion: MinIO → staging → curated",
    # Challenge File, Part 4: "Daily at 06:00 UTC"
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 4, 5),
    catchup=False,
    # Prevents concurrent runs from truncating staging mid-transform
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
    tags=["retail", "daily", "ingestion"],
    on_failure_callback=send_failure_alert,
    on_success_callback=dag_success_callback,
) as dag:

    # ----------------------------------------------------------
    # T1 — Sensor: wait for primary file to exist in MinIO
    # Challenge File, Part 2: "check_files_exist | S3KeySensor"
    # poke_mode + timeout: waits up to 30min for file to arrive
    # ----------------------------------------------------------
    check_files_exist = S3KeySensor(
        task_id        ="check_files_exist",
        bucket_name    ="{{ var.value.source_bucket }}",
        bucket_key="retail-data/{{ data_interval_start.strftime('%Y-%m-%d') }}/sales_transactions.csv",
        aws_conn_id    =MINIO_CONN_ID,
        poke_interval  =30,
        timeout        =1800,  # 30 minutes
        mode           ="poke",
        on_failure_callback=send_failure_alert,
    )

    # ----------------------------------------------------------
    # T2 — Branch: verify ALL 3 files exist
    # Challenge File, Part 2: "branch_on_files | BranchPythonOperator"
    # ----------------------------------------------------------
    branch_task = BranchPythonOperator(
        task_id        ="branch_on_files",
        python_callable=branch_on_files,
        on_failure_callback=send_failure_alert,
    )

    # ----------------------------------------------------------
    # T3a — Alert: dead-letter path when files missing
    # ----------------------------------------------------------
    alert_task = PythonOperator(
        task_id        ="alert_missing_files",
        python_callable=alert_missing_files,
        on_failure_callback=send_failure_alert,
    )

    # ----------------------------------------------------------
    # T3b — Ingest: MinIO CSVs → staging tables
    # Challenge File, Part 2: "ingest_to_staging | PythonOperator"
    # Retry 3x with 30s delay for transient network issues
    # ----------------------------------------------------------
    ingest_task = PythonOperator(
        task_id        ="ingest_to_staging",
        python_callable=ingest_to_staging,
        retries        =3,
        retry_delay    =timedelta(seconds=30),
        on_failure_callback=send_failure_alert,
    )

    # ----------------------------------------------------------
    # T4 — Transform: staging → curated
    # Challenge File, Part 3: "transform_sales | PythonOperator"
    # ----------------------------------------------------------
    transform_task = PythonOperator(
        task_id        ="transform_sales",
        python_callable=transform_sales,
        retries        =1,
        retry_delay    =timedelta(seconds=30),
        on_failure_callback=send_failure_alert,
    )

    # T5 — Load curated via PostgresOperator UPSERT
    # Challenge File, Part 2: "load_curated | PostgresOperator
    #   | Load curated dimension/fact tables"
    # Reads from staging.fact_daily_sales_stage written by
    # transform_sales. UPSERT on PRIMARY KEY ensures idempotency.
    # Challenge File Tips: "Think about idempotency"
    # ----------------------------------------------------------
    load_curated = PostgresOperator(
        task_id         ="load_curated",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql             ="""
            INSERT INTO curated.fact_daily_sales (
                store_id, product_id, transaction_date,
                category, subcategory, brand,
                total_quantity, total_revenue, total_tax,
                transaction_count, avg_discount, load_timestamp
            )
            SELECT
                store_id, product_id, transaction_date::DATE,
                category, subcategory, brand,
                total_quantity, total_revenue, total_tax,
                transaction_count, avg_discount, load_timestamp
            FROM staging.fact_daily_sales_stage
            ON CONFLICT (store_id, product_id, transaction_date)
            DO UPDATE SET
                total_quantity    = EXCLUDED.total_quantity,
                total_revenue     = EXCLUDED.total_revenue,
                total_tax         = EXCLUDED.total_tax,
                transaction_count = EXCLUDED.transaction_count,
                avg_discount      = EXCLUDED.avg_discount,
                load_timestamp    = EXCLUDED.load_timestamp;
        """,
        on_failure_callback=send_failure_alert,
    )


# ----------------------------------------------------------
    # GX1 — Validate staging after ingestion
    # Runs between ingest_to_staging and transform_sales
    # ----------------------------------------------------------
    validate_staging_task = PythonOperator(
        task_id        ="validate_staging",
        python_callable=validate_staging,
        retries        =1,
        retry_delay    =timedelta(seconds=30),
        on_failure_callback=send_failure_alert,
    )

    # ----------------------------------------------------------
    # GX2 — Validate curated after load
    # Runs between load_curated and archive_source_files
    # ----------------------------------------------------------
    validate_curated_task = PythonOperator(
        task_id        ="validate_curated",
        python_callable=validate_curated,
        retries        =1,
        retry_delay    =timedelta(seconds=30),
        on_failure_callback=send_failure_alert,
    )





    # ----------------------------------------------------------
    # T6 — Archive source files
    # ----------------------------------------------------------
    archive_task = PythonOperator(
        task_id        ="archive_source_files",
        python_callable=archive_source_files,
        retries        =2,
        retry_delay    =timedelta(seconds=30),
        on_failure_callback=send_failure_alert,
    )

    # ----------------------------------------------------------
    # T7 — Notify: always runs (success or failure)
    # Challenge File, Part 4: "trigger_rule=all_done"
    # Challenge File, Sample Diagram: final node
    # ----------------------------------------------------------
    notify_task = PythonOperator(
        task_id        ="send_notification",
        python_callable=send_notification,
        trigger_rule   =TriggerRule.ALL_DONE,
        on_failure_callback=send_failure_alert,
    )

    # ==========================================================
    # TASK DEPENDENCIES
    # Challenge File, Sample Diagram: full dependency map
    # ==========================================================
    check_files_exist >> branch_task
    branch_task       >> [ingest_task, alert_task]
    ingest_task       >> validate_staging_task >> transform_task
    transform_task    >> load_curated >> validate_curated_task
    validate_curated_task >> archive_task
    archive_task      >> notify_task
    alert_task        >> notify_task