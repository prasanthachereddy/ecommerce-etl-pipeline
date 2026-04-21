# ============================================================
# Airflow DAG — E-Commerce ETL Pipeline
# Author  : Chereddy Prasantha Reddy
# Project : E-Commerce ETL Pipeline (Airflow + AWS Glue + S3 + Athena)
# GitHub  : github.com/prasanthachereddy/ecommerce-etl-pipeline
# ============================================================
# Pipeline Flow:
#   [1] generate_data       → runs data_generator.py inside Docker
#   [2] upload_to_s3        → uploads CSVs to S3 raw/ folder
#   [3] start_raw_crawler   → triggers Glue crawler on raw/ folder
#   [4] wait_raw_crawler    → polls until raw crawler finishes
#   [5] start_glue_job      → triggers Glue ETL job (clean + transform)
#   [6] wait_glue_job       → polls until Glue job succeeds
#   [7] start_clean_crawler → triggers Glue crawler on clean/ folder
#   [8] wait_clean_crawler  → polls until clean crawler finishes
#   [9] run_athena_query    → runs analytics SQL on clean data
# ============================================================

import os
import sys
import logging
from datetime import datetime, timedelta

# ── Airflow core ──────────────────────────────────────────────
from airflow import DAG
from airflow.operators.python import PythonOperator

# ── AWS Operators & Sensors ───────────────────────────────────
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor

# ── Make data_generator importable from /opt/airflow/dags ────
sys.path.insert(0, "/opt/airflow/dags")
from data_generator import generate_data

log = logging.getLogger(__name__)

# ── PIPELINE CONFIG ───────────────────────────────────────────
# All settings in one place — change here, nothing else needs updating

BUCKET           = "sales-data-pipeline-prasantha"
RAW_PREFIX       = "raw"                          # s3://BUCKET/raw/
CLEAN_PREFIX     = "clean"                        # s3://BUCKET/clean/
ATHENA_OUTPUT    = f"s3://{BUCKET}/athena-results/"  # Athena writes query results here

RAW_CRAWLER      = "raw-data-crawler"             # Crawls s3://BUCKET/raw/
CLEAN_CRAWLER    = "processed_data_crawler"       # Crawls s3://BUCKET/clean/
GLUE_JOB         = "glue_clean_job"              # Cleans raw → writes to clean/

GLUE_DB          = "ecommerce_raw_db"                # Glue Data Catalog database name
                                                  # UPDATE this if your catalog DB has a different name

AWS_CONN_ID      = "aws_default"                 # Airflow Connection ID for AWS
                                                  # Set up in Airflow UI: Admin → Connections

LOCAL_OUTPUT_DIR = "/tmp/ecommerce"              # Where data_generator saves CSVs inside Docker
TARGET_ITEMS     = 100000                         # Number of order items to generate

SENSOR_TIMEOUT   = 60 * 60 * 2                   # 2 hours — max time to wait for crawler/Glue job
SENSOR_POKE      = 30                             # Poll AWS every 30 seconds

# ── DEFAULT TASK ARGUMENTS ────────────────────────────────────
default_args = {
    "owner":            "prasantha",
    "depends_on_past":  False,            # Each run is independent — no dependency on previous run
    "retries":          1,                # Retry once on failure before marking task as failed
    "retry_delay":      timedelta(minutes=5),  # Wait 5 minutes before retrying
    "email_on_failure": False,            # Set to True and add email in Airflow connections to get alerts
    "email_on_retry":   False,
}


# ═══════════════════════════════════════════════════════════════
# TASK FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def task_generate_data(**context) -> str:
    """
    Task 1 — Generates the e-commerce dataset using data_generator.py.

    Calls generate_data() which saves 5 CSV files to LOCAL_OUTPUT_DIR:
        /tmp/ecommerce/categories/categories_YYYYMMDD.csv
        /tmp/ecommerce/products/products_YYYYMMDD.csv
        /tmp/ecommerce/customers/customers_YYYYMMDD.csv
        /tmp/ecommerce/orders/orders_YYYYMMDD.csv
        /tmp/ecommerce/order_items/order_items_YYYYMMDD.csv

    Returns:
        LOCAL_OUTPUT_DIR — pushed to XCom automatically by Airflow.
        Task 2 pulls this path to know where to find the CSV files.
    """
    log.info("Task 1: Starting data generation...")
    output_dir = generate_data(
        output_dir=LOCAL_OUTPUT_DIR,
        target_items=TARGET_ITEMS
    )
    log.info(f"Task 1: Data generation complete. Files at: {output_dir}")
    return output_dir  # XCom push — Task 2 reads this


def task_upload_to_s3(**context) -> None:
    """
    Task 2 — Uploads all generated CSVs from Docker to S3 raw/ folder.

    Pulls LOCAL_OUTPUT_DIR from XCom (Task 1's return value).
    Walks the folder tree and uploads every .csv file preserving the subfolder structure:
        /tmp/ecommerce/customers/customers_20250415.csv
        → s3://sales-data-pipeline-prasantha/raw/customers/customers_20250415.csv

    Uses S3Hook — Airflow's built-in S3 client that reads AWS credentials
    from the Airflow Connection (aws_default).
    """
    # Pull the output path from Task 1 via XCom
    ti         = context["ti"]
    output_dir = ti.xcom_pull(task_ids="generate_data", key="return_value")

    if not output_dir:
        raise ValueError("XCom value from generate_data is empty. Task 1 may have failed silently.")

    if not os.path.exists(output_dir):
        raise FileNotFoundError(
            f"Output directory not found: {output_dir}. "
            f"Task 1 may have cleaned up on failure."
        )

    log.info(f"Task 2: Uploading files from {output_dir} to s3://{BUCKET}/{RAW_PREFIX}/")

    hook         = S3Hook(aws_conn_id=AWS_CONN_ID)
    upload_count = 0

    # os.walk() recursively finds every file in the folder tree
    for root, dirs, files in os.walk(output_dir):
        for filename in files:
            if not filename.endswith(".csv"):
                continue  # Skip any non-CSV files (e.g. .tmp files from interrupted writes)

            local_path = os.path.join(root, filename)

            # Build S3 key preserving subfolder structure
            # Example: /tmp/ecommerce/customers/customers_20250415.csv
            #   → relative: customers/customers_20250415.csv
            #   → s3 key:   raw/customers/customers_20250415.csv
            relative_path = os.path.relpath(local_path, output_dir)
            s3_key        = f"{RAW_PREFIX}/{relative_path}"

            hook.load_file(
                filename=local_path,
                key=s3_key,
                bucket_name=BUCKET,
                replace=True   # Overwrite if same filename exists (idempotent re-runs)
            )
            log.info(f"  Uploaded: {filename} → s3://{BUCKET}/{s3_key}")
            upload_count += 1

    if upload_count == 0:
        raise RuntimeError(f"No CSV files found in {output_dir}. Nothing was uploaded.")

    log.info(f"Task 2: Upload complete — {upload_count} files uploaded to S3.")


# ═══════════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════════

with DAG(
    dag_id="ecommerce_datagen_etl_pipeline",
    description="E-Commerce ETL: Generate → S3 → Crawler → Glue Job → Crawler → Athena",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),   # Historical start date — safe reference point
    schedule_interval="@weekly",        # Runs every Sunday at midnight UTC
    catchup=False,                      # Do NOT backfill past missed runs
    max_active_runs=1,                  # Only one pipeline run at a time — prevents S3 collisions
    tags=["ecommerce", "etl", "portfolio"],
) as dag:

    # ── TASK 1: Generate Data ──────────────────────────────────
    # Runs data_generator.py inside Docker → saves CSVs to /tmp/ecommerce/
    generate_data_task = PythonOperator(
        task_id="generate_data",
        python_callable=task_generate_data,
        provide_context=True,   # Passes **context (includes ti for XCom)
    )

    # ── TASK 2: Upload to S3 ──────────────────────────────────
    # Reads /tmp/ecommerce/ → uploads all CSVs to s3://BUCKET/raw/
    upload_to_s3_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=task_upload_to_s3,
        provide_context=True,
    )

    # ── TASK 3: Start Raw Crawler ─────────────────────────────
    # Triggers the Glue Crawler that scans s3://BUCKET/raw/
    # The crawler detects schema and registers tables in Glue Data Catalog
    start_raw_crawler = GlueCrawlerOperator(
        task_id="start_raw_crawler",
        config={"Name": RAW_CRAWLER},   # Crawler must already exist in AWS Glue console
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=False,       # Do not block here — Task 4 (sensor) handles waiting
    )

    # ── TASK 4: Wait for Raw Crawler ─────────────────────────
    # Polls every 30 seconds until raw-data-crawler status = READY
    # Separate from Task 3 for better visibility in Airflow UI
    wait_raw_crawler = GlueCrawlerSensor(
        task_id="wait_raw_crawler",
        crawler_name=RAW_CRAWLER,
        aws_conn_id=AWS_CONN_ID,
        poke_interval=SENSOR_POKE,      # Check every 30 seconds
        timeout=SENSOR_TIMEOUT,          # Fail if not done within 2 hours
        mode="reschedule",               # Releases worker slot between pokes — efficient
    )

    # ── TASK 5: Start Glue Job ────────────────────────────────
    # Triggers the Glue ETL job that:
    #   - Reads from s3://BUCKET/raw/
    #   - Cleans dirty data (invalid FKs, negative quantities, future dates)
    #   - Writes clean Parquet to s3://BUCKET/clean/
    start_glue_job = GlueJobOperator(
        task_id="start_glue_job",
        job_name=GLUE_JOB,
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=False,       # Task 6 (sensor) handles waiting
        # job_desc is optional but shows in Glue console — useful for debugging
        script_args={
            "--BUCKET":       BUCKET,
            "--RAW_PREFIX":   RAW_PREFIX,
            "--CLEAN_PREFIX": CLEAN_PREFIX,
        },
    )

    # ── TASK 6: Wait for Glue Job ─────────────────────────────
    # Polls until the Glue job status = SUCCEEDED
    # If Glue job fails, this sensor marks the Airflow task as FAILED too
    wait_glue_job = GlueJobSensor(
        task_id="wait_glue_job",
        job_name=GLUE_JOB,
        # Pulls the Glue job run_id from Task 5's XCom return value
        run_id="{{ task_instance.xcom_pull(task_ids='start_glue_job', key='return_value') }}",
        aws_conn_id=AWS_CONN_ID,
        poke_interval=SENSOR_POKE,
        timeout=SENSOR_TIMEOUT,
        mode="reschedule",
    )

    # ── TASK 7: Start Clean Crawler ───────────────────────────
    # Triggers the Glue Crawler that scans s3://BUCKET/clean/
    # Registers the cleaned tables in Glue Data Catalog
    # Athena queries this catalog — it must be updated after every Glue job run
    start_clean_crawler = GlueCrawlerOperator(
        task_id="start_clean_crawler",
        config={"Name": CLEAN_CRAWLER},
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=False,
    )

    # ── TASK 8: Wait for Clean Crawler ───────────────────────
    wait_clean_crawler = GlueCrawlerSensor(
        task_id="wait_clean_crawler",
        crawler_name=CLEAN_CRAWLER,
        aws_conn_id=AWS_CONN_ID,
        poke_interval=SENSOR_POKE,
        timeout=SENSOR_TIMEOUT,
        mode="reschedule",
    )

    # ── TASK 9: Run Athena Query ──────────────────────────────
    # Runs an analytics SQL query on the clean data via Athena
    # Athena reads directly from s3://BUCKET/clean/ (serverless — no cluster needed)
    # Results are saved to s3://BUCKET/athena-results/
    run_athena_query = AthenaOperator(
        task_id="run_athena_query",
        query="""
            -- Total revenue by product category (clean data only)
            -- This proves the pipeline produced correct, queryable data
            SELECT
    c.category_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.quantity) AS total_units_sold,
    ROUND(
        SUM(CAST(oi.final_price AS DECIMAL(18,2)) * oi.quantity),
        2
    ) AS total_revenue_inr
FROM processed_order_items oi
JOIN processed_orders o ON oi.order_id = o.order_id
JOIN processed_products p ON oi.product_id = p.product_id
JOIN processed_categories c ON p.category_id = c.category_id
WHERE o.order_status = 'completed'
GROUP BY c.category_name
ORDER BY total_revenue_inr DESC;
        """
        ,
        database="ecommerce_raw_db",                       # Glue Data Catalog database — update if different
        output_location=ATHENA_OUTPUT,           # Where Athena saves query results CSV
        aws_conn_id=AWS_CONN_ID,
        region_name="ap-south-2",
        sleep_time=SENSOR_POKE,                  # Poll Athena query status every 30 seconds
    )

    # ═══════════════════════════════════════════════════════════
    # TASK DEPENDENCIES — defines the order of execution
    # Read as: >> means "then run"
    # ═══════════════════════════════════════════════════════════
    (
        generate_data_task
        >> upload_to_s3_task
        >> start_raw_crawler
        >> wait_raw_crawler
        >> start_glue_job
        >> wait_glue_job
        >> start_clean_crawler
        >> wait_clean_crawler
        >> run_athena_query
    )
