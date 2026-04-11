# FILE: retail_daily_ingestion.py
# DELIVERABLE: diluksha-perera-month1-airflow-dag.py
# Session File, Learning Objectives: "Design and build Apache
#   Airflow DAGs with multiple operators and task dependencies"

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
from airflow.providers.http.hooks.http import HttpHook

log = logging.getLogger(__name__)

# DEFAULT ARGS
default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "email": ["diluksha@bistcglobal.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "sla": timedelta(hours=2),
}

# CONSTANTS
# Using Variable.get so it reads from Airflow UI — not hardcoded
MINIO_CONN_ID    = "minio_s3"
POSTGRES_CONN_ID = "postgres_retail"
RUN_DATE_MACRO   = "{{ ds }}"  # Airflow execution date YYYY-MM-DD

# These are the 3 source files
SOURCE_FILES = [
    "sales_transactions.csv",
    "product_catalog.csv",
    "store_locations.csv",
]


# HELPER: failure alert callback
def send_failure_alert(context):
    """
    Posts failure alert to Slack via webhook connection.
    Challenge File, Part 4: "on_failure_callback, alerting,
      set up email/Slack notifications on task failure"
    Connection: slack_webhook (Admin -> Connections in Airflow UI)
    Secrets stored in Airflow connection — not hardcoded.
    Challenge File, Common Mistakes: "Hardcoding connection strings"
    """
    dag_id    = context["dag"].dag_id
    task_id   = context["task_instance"].task_id
    run_date  = context.get("ds", "unknown")
    exception = str(context.get("exception", "No exception details"))

    message = {
        "text": ":red_circle: *Pipeline Failure Alert*",
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
        http_hook = HttpHook(method="POST", http_conn_id="slack_webhook")
        conn = http_hook.get_connection("slack_webhook")
        webhook_path = conn.password
        http_hook.run(
            endpoint=webhook_path,
            data=json.dumps(message),
            headers={"Content-Type": "application/json"},
        )
        log.info("Slack alert sent for task: %s", task_id)
    except Exception as e:
        log.error("Slack notification failed: %s", str(e))


# TASK 1 — BRANCH: check all 3 files exist
# WHY: S3KeySensor checks ONE file. We need ALL 3 confirmed
#   before ingestion. Missing file = broken joins downstream.
def branch_on_files(**context) -> str:
    source_bucket = Variable.get("source_bucket", default_var="raw-data")
    run_date      = context["data_interval_start"].strftime("%Y-%m-%d")
    s3_hook       = S3Hook(aws_conn_id=MINIO_CONN_ID)

    missing = []
    for fname in SOURCE_FILES:
        key = f"retail-data/{run_date}/{fname}"
        if not s3_hook.check_for_key(key=key, bucket_name=source_bucket):
            missing.append(key)
            log.warning("Missing file: s3://%s/%s", source_bucket, key)

    if missing:
        log.error("Branch -> alert_missing_files. Missing: %s", missing)
        return "alert_missing_files"

    log.info("Branch -> ingest_to_staging. All 3 files confirmed.")
    return "ingest_to_staging"


# TASK 2 — ALERT: missing files dead-letter path
def alert_missing_files(**context):
    run_date = context["data_interval_start"].strftime("%Y-%m-%d")
    raise Exception(
        f"Pipeline aborted: one or more source files missing "
        f"for run_date={run_date}. Check MinIO bucket."
    )


# TASK 3 — INGEST: download CSVs from MinIO -> staging tables
def ingest_to_staging(**context):
    """
    Downloads all 3 pipe-delimited CSVs from MinIO and loads
    them into staging tables. Uses TRUNCATE + INSERT for
    idempotency — safe to rerun without duplicating data.
    Challenge File Tips: "Think about idempotency"
    """
    source_bucket = Variable.get("source_bucket", default_var="raw-data")
    run_date      = context["data_interval_start"].strftime("%Y-%m-%d")
    s3_hook       = S3Hook(aws_conn_id=MINIO_CONN_ID)
    pg_hook       = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

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

        file_content = s3_hook.read_key(key=key, bucket_name=source_bucket)
        df = pd.read_csv(io.StringIO(file_content), delimiter="|")

        if df.empty:
            raise Exception(f"File {fname} loaded 0 rows — aborting.")

        log.info("File %s loaded: %d rows", fname, len(df))

        # Replace NaN with None so PostgreSQL gets NULL not "nan"
        df = df.where(pd.notna(df), None)

        # TRUNCATE then INSERT — idempotency pattern
        pg_hook.run(f"TRUNCATE TABLE {config['table']};")
        pg_hook.insert_rows(
            table=config["table"],
            rows=df[config["columns"]].values.tolist(),
            target_fields=config["columns"],
        )
        log.info("Staged %d rows -> %s", len(df), config["table"])

    # Row count anomaly check
    count = pg_hook.get_first(
        "SELECT COUNT(*) FROM staging.sales_transactions;"
    )[0]
    log.info("staging.sales_transactions row count: %d", count)
    if count < 100:
        raise AirflowException(
            f"Anomaly detected: only {count} rows in "
            f"staging.sales_transactions. Expected >=100. "
            f"Investigate source file."
        )


# TASK 4 — TRANSFORM: business logic on staging data
def transform_sales(**context):
    """
    Reads from staging, applies 4 transforms, writes enriched
    data to staging.fact_daily_sales_stage for PostgresOperator
    UPSERT into curated.fact_daily_sales.

    Transforms applied (Challenge File, Part 3):
      1. Filter    — remove nulls/invalid, log to error_log
      2. Derived   — line_total, tax_amount, total_with_tax
      3. Aggregate — daily summary by store + product
      4. Lookup    — enrich with product category from catalog
    """
    run_date = context["data_interval_start"].strftime("%Y-%m-%d")
    pg_hook  = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    df = pg_hook.get_pandas_df("SELECT * FROM staging.sales_transactions;")
    log.info("Transform input: %d rows from staging", len(df))

    # --- Transform 1: Filter ---
    before   = len(df)
    bad_mask = ~(
        df["transaction_id"].notna() &
        (df["quantity"].astype(float) > 0) &
        (df["unit_price"].astype(float) > 0)
    )
    bad_rows = df[bad_mask].copy()

    if not bad_rows.empty:
        error_rows = []
        for _, bad_row in bad_rows.iterrows():
            error_rows.append((
                "staging.sales_transactions",
                bad_row.to_json(),
                "Failed filter: null transaction_id "
                "or quantity/unit_price <= 0",
            ))
        pg_hook.insert_rows(
            table="staging.error_log",
            rows=error_rows,
            target_fields=["source_table", "raw_data", "error_message"],
        )
        log.warning("Wrote %d invalid rows to staging.error_log", len(bad_rows))

    df = df[~bad_mask].copy()
    removed = before - len(df)
    if removed > 0:
        log.warning("Filter removed %d invalid rows", removed)

    if df.empty:
        raise AirflowException("No valid rows after filter — aborting transform.")

    # Cast numeric columns
    df["quantity"]     = df["quantity"].astype(int)
    df["unit_price"]   = df["unit_price"].astype(float)
    df["discount_pct"] = df["discount_pct"].astype(float)

    # --- Transform 2: Derived Columns ---
    df["line_total"]     = (
        df["quantity"] * df["unit_price"] * (1 - df["discount_pct"] / 100)
    ).round(2)
    df["tax_amount"]     = (df["line_total"] * 0.08).round(2)
    df["total_with_tax"] = (df["line_total"] + df["tax_amount"]).round(2)
    df["load_timestamp"] = pd.Timestamp.now()
    df["source_file"]    = f"sales_transactions_{run_date}.csv"

    # --- Transform 3: Aggregate ---
    daily_summary = df.groupby(
        ["store_id", "product_id", "transaction_date"]
    ).agg(
        total_quantity    =("quantity",      "sum"),
        total_revenue     =("line_total",    "sum"),
        total_tax         =("tax_amount",    "sum"),
        transaction_count =("transaction_id","count"),
        avg_discount      =("discount_pct",  "mean"),
    ).reset_index()

    daily_summary["total_revenue"]  = daily_summary["total_revenue"].round(2)
    daily_summary["total_tax"]      = daily_summary["total_tax"].round(2)
    daily_summary["avg_discount"]   = daily_summary["avg_discount"].round(2)
    daily_summary["load_timestamp"] = pd.Timestamp.now()

    log.info("Aggregated to %d store-product-date rows", len(daily_summary))

    # --- Transform 4: Lookup / Enrich ---
    product_df = pg_hook.get_pandas_df(
        "SELECT product_id, category, subcategory, brand "
        "FROM staging.product_catalog;"
    )
    enriched   = daily_summary.merge(product_df, on="product_id", how="left")
    unmatched  = enriched["category"].isna().sum()
    if unmatched > 0:
        log.warning("%d rows have no matching product in catalog", unmatched)

    log.info("Enriched dataset: %d rows ready for curated load", len(enriched))

    # --- Write to staging.fact_daily_sales_stage ---
    # PostgresOperator (load_curated) UPSERTs from this table
    pg_hook.run("TRUNCATE TABLE staging.fact_daily_sales_stage;")

    rows = []
    for _, row in enriched.iterrows():
        rows.append((
            str(row["store_id"]),
            str(row["product_id"]),
            str(row["transaction_date"]),
            str(row["category"])    if pd.notna(row.get("category"))    else None,
            str(row["subcategory"]) if pd.notna(row.get("subcategory")) else None,
            str(row["brand"])       if pd.notna(row.get("brand"))       else None,
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


# TASK 5 — ARCHIVE: move processed files to archive bucket
def archive_source_files(**context):
    source_bucket  = Variable.get("source_bucket", default_var="raw-data")
    archive_bucket = "archive-data"
    run_date       = context["data_interval_start"].strftime("%Y-%m-%d")
    s3_hook        = S3Hook(aws_conn_id=MINIO_CONN_ID)

    try:
        s3_hook.get_conn().create_bucket(Bucket=archive_bucket)
        log.info("Created archive bucket: %s", archive_bucket)
    except Exception as e:
        log.info("Archive bucket note: %s", str(e))

    for fname in SOURCE_FILES:
        source_key  = f"retail-data/{run_date}/{fname}"
        archive_key = f"retail-data/{run_date}/{fname}"
        s3_hook.copy_object(
            source_bucket_key  =source_key,
            dest_bucket_key    =archive_key,
            source_bucket_name =source_bucket,
            dest_bucket_name   =archive_bucket,
        )
        log.info("Archived: %s -> s3://%s/%s",
                 source_key, archive_bucket, archive_key)


# TASK 6 — NOTIFICATION: success/failure summary via Slack
def send_notification(**context):
    run_date     = context["data_interval_start"].strftime("%Y-%m-%d")
    tis          = context["dag_run"].get_task_instances()
    failed_tasks = [
        ti.task_id for ti in tis
        if ti.state in ("failed", "upstream_failed")
        and ti.task_id != "send_notification"
    ]
    state = "failed" if failed_tasks else "success"
    emoji = ":white_check_mark:" if state == "success" else ":red_circle:"

    message = {
        "text": f"{emoji} Pipeline {state.upper()}: retail_daily_ingestion",
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"{emoji} Pipeline {state.upper()}"}
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:*\nretail_daily_ingestion"},
                    {"type": "mrkdwn", "text": f"*Run Date:*\n{run_date}"},
                    {"type": "mrkdwn", "text": f"*Status:*\n{state.upper()}"},
                    {"type": "mrkdwn", "text": f"*Failed Tasks:*\n{', '.join(failed_tasks) if failed_tasks else 'None'}"},
                ]
            }
        ]
    }

    try:
        http_hook    = HttpHook(method="POST", http_conn_id="slack_webhook")
        conn         = http_hook.get_connection("slack_webhook")
        webhook_path = conn.password
        http_hook.run(
            endpoint=webhook_path,
            data=json.dumps(message),
            headers={"Content-Type": "application/json"},
        )
        log.info("Slack notification sent | state: %s", state)
    except Exception as e:
        log.error("Slack notification failed: %s", str(e))


# DAG-LEVEL SUCCESS CALLBACK
def dag_success_callback(context):
    log.info(
        "DAG completed successfully | DAG: %s | Run: %s",
        context["dag"].dag_id,
        context["run_id"],
    )


# DAG DEFINITION
# max_active_runs=1 — prevents concurrent runs from truncating
#   staging while a previous transform is still reading it
with DAG(
    dag_id           ="retail_daily_ingestion",
    default_args     =default_args,
    description      ="Retail POS daily ingestion: MinIO -> staging -> curated",
    schedule_interval="0 6 * * *",
    start_date       =datetime(2026, 4, 5),
    catchup          =False,
    max_active_runs  =1,
    dagrun_timeout   =timedelta(hours=4),
    tags             =["retail", "daily", "ingestion"],
    on_failure_callback=send_failure_alert,
    on_success_callback=dag_success_callback,
) as dag:

    # T1 — S3KeySensor: wait for primary file in MinIO
    check_files_exist = S3KeySensor(
        task_id    ="check_files_exist",
        bucket_name="{{ var.value.source_bucket }}",
        bucket_key ="retail-data/{{ data_interval_start.strftime('%Y-%m-%d') }}/sales_transactions.csv",
        aws_conn_id=MINIO_CONN_ID,
        poke_interval=30,
        timeout    =1800,
        mode       ="poke",
        on_failure_callback=send_failure_alert,
    )

    # T2 — BranchPythonOperator: verify ALL 3 files exist
    branch_task = BranchPythonOperator(
        task_id        ="branch_on_files",
        python_callable=branch_on_files,
        on_failure_callback=send_failure_alert,
    )

    # T3a — Alert: dead-letter path when files missing
    alert_task = PythonOperator(
        task_id        ="alert_missing_files",
        python_callable=alert_missing_files,
        on_failure_callback=send_failure_alert,
    )

    # T3b — Ingest: MinIO CSVs -> staging tables
    ingest_task = PythonOperator(
        task_id        ="ingest_to_staging",
        python_callable=ingest_to_staging,
        retries        =3,
        retry_delay    =timedelta(seconds=30),
        on_failure_callback=send_failure_alert,
    )

    # T4 — Transform: staging -> curated stage table
    transform_task = PythonOperator(
        task_id        ="transform_sales",
        python_callable=transform_sales,
        retries        =1,
        retry_delay    =timedelta(seconds=30),
        on_failure_callback=send_failure_alert,
    )

    # T5 — Load curated via PostgresOperator UPSERT
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

    # T6 — Archive source files
    archive_task = PythonOperator(
        task_id        ="archive_source_files",
        python_callable=archive_source_files,
        retries        =2,
        retry_delay    =timedelta(seconds=30),
        on_failure_callback=send_failure_alert,
    )

    # T7 — Notify: always runs (success or failure)
    notify_task = PythonOperator(
        task_id        ="send_notification",
        python_callable=send_notification,
        trigger_rule   =TriggerRule.ALL_DONE,
        on_failure_callback=send_failure_alert,
    )

    # TASK DEPENDENCIES
    check_files_exist >> branch_task
    branch_task       >> [ingest_task, alert_task]
    ingest_task       >> transform_task >> load_curated >> archive_task
    archive_task      >> notify_task
    alert_task        >> notify_task
