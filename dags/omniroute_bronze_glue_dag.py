"""
OmniRoute — Bronze Ingestion DAG (Glue-based)
==============================================
Schedule : Daily @ 05:00 UTC
Trigger  : Single AWS Glue job that ingests all 3 Bronze datasets

This DAG reads S3 paths and Glue job configuration from s3_paths.json,
then triggers the `omniroute-daily-ingest-bronze` Glue job with those
paths passed as Glue job arguments.

Pipeline:
    S3 Landing (CSV) → Glue Job → S3 Ingested (Parquet)
                                 └→ S3 Quarantine (invalid files)

DAG Dependency Graph:
═════════════════════
    start_task → trigger_glue_bronze_ingest → end_task

Key Design Decisions:
    1. All 3 datasets (fuel_transactions, vehicle_registry, vehicle_assignment)
       are processed in a SINGLE Glue job to minimize cold-start costs.
    2. S3 paths are loaded from s3_paths.json at DAG parse time — this keeps
       configuration centralized and avoids hardcoding paths in DAG code.
    3. The DAG uses GlueJobOperator from the AWS provider to trigger the job
       and poll for completion automatically.

Prerequisites:
    - pip install apache-airflow-providers-amazon
    - The Glue job must be created in AWS Glue console (or via Terraform/CDK)
      with the script uploaded to the S3 location in s3_paths.json → glue.jobs.bronze_ingest.script_location
    - The IAM role specified in s3_paths.json → glue.jobs.bronze_ingest.iam_role_name
      must have permissions to read/write the Bronze S3 paths
    - An Airflow AWS connection (conn_id='aws_default') must be configured
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

# ──────────────────────────────────────────────────────────────
# Airflow Imports
# ──────────────────────────────────────────────────────────────
# Airflow 3.x: core authoring objects are in airflow.sdk
from airflow.sdk import DAG

# EmptyOperator replaces DummyOperator in Airflow 2.4+
# Used for visual clarity as start/end markers in the DAG graph
from airflow.providers.standard.operators.empty import EmptyOperator

# GlueJobOperator triggers an AWS Glue ETL job and waits for it to complete.
# It handles polling, status checking, and failure propagation automatically.
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


# ──────────────────────────────────────────────────────────────
# Load Configuration from s3_paths.json
# ──────────────────────────────────────────────────────────────
# Resolve the path to s3_paths.json relative to this DAG file's location.
# This works regardless of where Airflow deploys the DAGs directory.
#
# Directory structure:
#   omniroute/
#   ├── dags/
#   │   └── omniroute_bronze_glue_dag.py   ← this file
#   ├── s3_paths.json                       ← config file (one level up)
#   └── glue_jobs/
#       └── daily_ingest_bronze_glue.py     ← Glue script

CONFIG_PATH = Path(os.path.dirname(os.path.abspath(__file__))).parent / "s3_paths.json"

with open(CONFIG_PATH, "r") as f:
    config = json.load(f)

# ── Extract Bronze S3 paths from the config ──
# These are passed as Glue job arguments so the Glue script knows
# where to read CSVs and write Parquet files.
BRONZE_LANDING_PATH = config["bronze"]["landing"]         # CSV source
BRONZE_INGESTED_PATH = config["bronze"]["ingested"]       # Parquet destination
BRONZE_QUARANTINE_PATH = config["bronze"]["quarantine"]   # Rejected files

# ── Extract Glue job configuration ──
# These settings control which Glue job to trigger and how it runs.
GLUE_CONFIG = config["glue"]["jobs"]["bronze_ingest"]
GLUE_JOB_NAME = GLUE_CONFIG["job_name"]                  # Glue job name in AWS console
GLUE_IAM_ROLE = GLUE_CONFIG["iam_role_name"]              # IAM role for Glue execution
GLUE_SCRIPT_LOCATION = GLUE_CONFIG["script_location"]     # S3 path to the Glue script
GLUE_VERSION = GLUE_CONFIG.get("glue_version", "4.0")     # Glue runtime version
WORKER_TYPE = GLUE_CONFIG.get("worker_type", "G.1X")      # Worker instance type
NUM_WORKERS = GLUE_CONFIG.get("number_of_workers", 2)     # Number of Glue workers
TIMEOUT_MINUTES = GLUE_CONFIG.get("timeout_minutes", 30)  # Job timeout

# ── Airflow Jinja template for execution date ──
# {{ ds }} resolves to the DAG's logical execution date (YYYY-MM-DD).
# For a run triggered on 2026-04-24 at 05:00, ds = "2026-04-24".
RUN_DATE = "{{ ds }}"

# ── AWS Connection ID configured in Airflow ──
# This must match the connection set up in Airflow UI → Admin → Connections
AWS_CONN_ID = "aws_default"


# ──────────────────────────────────────────────────────────────
# DAG Default Arguments
# ──────────────────────────────────────────────────────────────
# These settings apply to ALL tasks in the DAG unless overridden
# at the task level.
default_args = {
    "owner": "omniroute",              # Shown in Airflow UI for filtering
    "depends_on_past": False,          # Each run is independent of previous runs
    "retries": 2,                      # Retry failed tasks up to 2 times
    "retry_delay": timedelta(minutes=5),  # Wait 5 min between retries
    "email_on_failure": False,         # Disable email alerts (configure SNS/Slack instead)
}


# ──────────────────────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────────────────────
with DAG(
    dag_id="omniroute_bronze_glue_ingest",
    description=(
        "Daily Bronze ingestion via AWS Glue — reads CSV from landing, "
        "validates schema, writes Parquet to ingested zone"
    ),
    schedule="0 5 * * *",               # Run daily at 05:00 UTC
    start_date=datetime(2026, 4, 1),     # DAG becomes eligible from this date
    catchup=False,                       # Don't backfill past dates on first deploy
    tags=["bronze", "glue", "daily", "ingestion"],
    default_args=default_args,
    max_active_runs=1,                   # Prevent parallel DAG runs (data consistency)
) as dag:

    # ──────────────────────────────────────────
    # Task 1: Start Marker
    # ──────────────────────────────────────────
    # Visual indicator in the Airflow DAG graph — no actual work.
    # Useful for adding pre-checks or sensors in the future.
    start = EmptyOperator(
        task_id="start",
    )

    # ──────────────────────────────────────────
    # Task 2: Trigger AWS Glue Bronze Ingestion
    # ──────────────────────────────────────────
    # This is the core task — it triggers the Glue job and waits
    # for it to complete (or fail).
    #
    # How it works:
    #   1. GlueJobOperator calls AWS Glue StartJobRun API
    #   2. Passes script_args as --arguments to the Glue job
    #   3. Polls GetJobRun every 30s until the job completes
    #   4. If the Glue job fails, this Airflow task also fails
    #
    # The script_args dictionary maps to the Glue job's
    # getResolvedOptions() parameters in the Glue script.
    trigger_glue_bronze_ingest = GlueJobOperator(
        task_id="trigger_glue_bronze_ingest",

        # ── Glue Job Identity ──
        job_name=GLUE_JOB_NAME,              # Must match the job name in AWS Glue console
        iam_role_name=GLUE_IAM_ROLE,          # IAM role assumed by Glue during execution
        script_location=GLUE_SCRIPT_LOCATION, # S3 path to the Glue Python script
        region_name="us-east-1",              # AWS region where the Glue job runs

        # ── Glue Job Runtime Configuration ──
        # create_job_kwargs provides additional CreateJob parameters
        # that aren't directly exposed as GlueJobOperator arguments.
        create_job_kwargs={
            "GlueVersion": GLUE_VERSION,      # Glue 4.0 = Spark 3.3 + Python 3.10
            "NumberOfWorkers": NUM_WORKERS,    # Number of DPUs allocated
            "WorkerType": WORKER_TYPE,         # G.1X = 1 DPU per worker (4 vCPU, 16 GB)
        },

        # ── Script Arguments ──
        # These are the custom parameters passed to the Glue script.
        # They are accessible via getResolvedOptions() in the script.
        # The '--' prefix is required by Glue for custom arguments.
        script_args={
            "--run_date": RUN_DATE,                       # Execution date from Airflow
            "--landing_path": BRONZE_LANDING_PATH,        # S3 CSV source
            "--ingested_path": BRONZE_INGESTED_PATH,      # S3 Parquet destination
            "--quarantine_path": BRONZE_QUARANTINE_PATH,  # S3 quarantine for bad files
            "--archive_path": config["bronze"]["archive"],# S3 archive path for processed files
        },

        # ── Polling & Timeout ──
        wait_for_completion=True,             # Block until Glue job finishes
        verbose=True,                         # Log Glue job progress to Airflow logs

        # ── AWS Connection ──
        aws_conn_id=AWS_CONN_ID,              # Airflow connection with AWS credentials

        # ── Retry Behavior ──
        # In addition to Airflow-level retries (default_args),
        # the Glue job itself can be configured to retry internally.
        # We keep Glue retries at 0 and let Airflow handle retries
        # for better observability in the Airflow UI.
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    # ──────────────────────────────────────────
    # Task 3: End Marker
    # ──────────────────────────────────────────
    # Visual indicator that the pipeline completed successfully.
    # Future: add data quality checks, notifications, or
    # downstream Silver triggers here.
    end = EmptyOperator(
        task_id="end",
    )

    # ──────────────────────────────────────────
    # Task Dependencies
    # ──────────────────────────────────────────
    # Linear flow: start → trigger Glue job → end
    # The start/end markers make it easy to add pre/post tasks later.
    start >> trigger_glue_bronze_ingest >> end
