"""
OmniRoute — Yearly Bronze Ingestion DAG (Glue-based)
=====================================================
Schedule : Yearly (e.g., January 1st)
Trigger  : AWS Glue job that ingests the maintenance schedules dataset.

This DAG reads S3 paths and Glue job configuration from s3_paths.json,
then triggers the `omniroute-yearly-ingest-bronze` Glue job with those
paths passed as arguments.

Pipeline:
    S3 Landing (CSV) → Glue Job → S3 Ingested (Parquet)
                                 └→ S3 Quarantine (invalid files)
                                 └→ S3 Archive (valid files post-processing)
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

# ──────────────────────────────────────────────────────────────
# Load Configuration from s3_paths.json
# ──────────────────────────────────────────────────────────────
CONFIG_PATH = Path(os.path.dirname(os.path.abspath(__file__))).parent / "s3_paths.json"

with open(CONFIG_PATH, "r") as f:
    config = json.load(f)

# ── Extract Bronze S3 paths ──
BRONZE_LANDING_PATH = config["bronze"]["landing"]
BRONZE_INGESTED_PATH = config["bronze"]["ingested"]
BRONZE_QUARANTINE_PATH = config["bronze"]["quarantine"]
BRONZE_ARCHIVE_PATH = config["bronze"]["archive"]

# ── Extract Glue job configuration ──
GLUE_CONFIG = config["glue_yearly"]["jobs"]["yearly_bronze_ingest"]
GLUE_JOB_NAME = GLUE_CONFIG["job_name"]
GLUE_IAM_ROLE = GLUE_CONFIG["iam_role_name"]
GLUE_SCRIPT_LOCATION = GLUE_CONFIG["script_location"]
GLUE_VERSION = GLUE_CONFIG.get("glue_version", "4.0")
WORKER_TYPE = GLUE_CONFIG.get("worker_type", "G.1X")
NUM_WORKERS = GLUE_CONFIG.get("number_of_workers", 2)
TIMEOUT_MINUTES = GLUE_CONFIG.get("timeout_minutes", 15)

RUN_DATE = "{{ ds }}"
AWS_CONN_ID = "aws_default"

default_args = {
    "owner": "omniroute",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="omniroute_yearly_bronze_glue_ingest",
    description="Yearly Maintenance Schedules ingestion via AWS Glue",
    schedule="0 0 1 1 *",                # Run at midnight on Jan 1st
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "glue", "yearly", "maintenance"],
    default_args=default_args,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    trigger_glue_yearly_ingest = GlueJobOperator(
        task_id="trigger_glue_yearly_ingest",
        job_name=GLUE_JOB_NAME,
        iam_role_name=GLUE_IAM_ROLE,
        script_location=GLUE_SCRIPT_LOCATION,
        region_name="us-east-1",

        create_job_kwargs={
            "GlueVersion": GLUE_VERSION,
            "NumberOfWorkers": NUM_WORKERS,
            "WorkerType": WORKER_TYPE,
        },

        script_args={
            "--run_date": RUN_DATE,
            "--landing_path": BRONZE_LANDING_PATH,
            "--ingested_path": BRONZE_INGESTED_PATH,
            "--quarantine_path": BRONZE_QUARANTINE_PATH,
            "--archive_path": BRONZE_ARCHIVE_PATH,
        },

        wait_for_completion=True,
        verbose=True,
        aws_conn_id=AWS_CONN_ID,
        retries=1,
        retry_delay=timedelta(minutes=3),
    )

    end = EmptyOperator(task_id="end")

    start >> trigger_glue_yearly_ingest >> end
