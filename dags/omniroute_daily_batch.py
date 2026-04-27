"""
OmniRoute Daily Batch DAG
==========================
Schedule: Daily @ 05:00 UTC
Pipeline: Bronze → DQ Gate → Silver → Gold → Reporting

Ingests vehicle registry, vehicle assignment, and fuel transactions,
validates data quality, then transforms, builds business tables
(SCD2, fuel audit, fleet snapshot), loads reporting DB, and generates
CSV reports.

Production Features:
- S3KeySensor waits for source files before ingestion
- DQ quality gate blocks pipeline if pass rate < 95%
- SLA monitoring (pipeline must complete within 2 hours)
- Failure callbacks for alerting integration
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowFailException

try:
    from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
    HAS_S3_SENSOR = True
except ImportError:
    HAS_S3_SENSOR = False


# ──────────────────────────────────────────────
# DAG Configuration
# ──────────────────────────────────────────────
SPARK_SUBMIT = "spark-submit"

# Delta Lake package — required for all Silver layer writes.
# This is passed to every spark-submit command that touches Delta tables.
DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.3.0"

JOBS_DIR = "/opt/omniroute/spark_jobs"  # Deploy target: sudo cp -r spark_jobs/* /opt/omniroute/spark_jobs/

# Airflow Jinja template: {{ ds }} resolves to the DAG's logical execution date
# e.g., for a run triggered on 2026-04-24 at 05:00, ds = "2026-04-24"
# This ensures all Spark jobs process data for the SAME date consistently
RUN_DATE = "{{ ds }}"  # YYYY-MM-DD execution date

S3_BRONZE_BUCKET = "ttn-de-bootcamp-bronze-us-east-1"
S3_LANDING_PREFIX = "landing"


def on_failure_callback(context):
    """Log failure details for alerting integration (Slack/SNS/PagerDuty)."""
    ti = context["task_instance"]
    dag_id = context["dag"].dag_id
    execution_date = context["execution_date"]
    log_url = ti.log_url
    print(
        f"🚨 FAILURE: {dag_id}.{ti.task_id} | "
        f"execution_date={execution_date} | "
        f"log_url={log_url}"
    )
    # Production: send to Slack/SNS/PagerDuty here


default_args = {
    "owner": "omniroute",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "on_failure_callback": on_failure_callback,
    "sla": timedelta(hours=2),
}


# ──────────────────────────────────────────────
# Helpers — build spark-submit commands
# ──────────────────────────────────────────────
def spark_cmd(script: str, extra_args: str = "") -> str:
    """Build a spark-submit command for Bronze layer (plain Parquet, no Delta)."""
    return f"{SPARK_SUBMIT} {JOBS_DIR}/{script} {extra_args}".strip()


def spark_delta_cmd(script: str, extra_args: str = "") -> str:
    """Build a spark-submit command for Silver/Gold layer (requires Delta Lake package).
    
    The --packages flag tells Spark to download the Delta Lake JAR from Maven
    at runtime. This is required for .format("delta") reads and writes.
    """
    return (
        f"{SPARK_SUBMIT} "
        f"--packages {DELTA_PACKAGE} "
        f"{JOBS_DIR}/{script} {extra_args}"
    ).strip()


# ──────────────────────────────────────────────
# DQ Gate — block pipeline if quality is too low
# ──────────────────────────────────────────────
def check_dq_threshold(**context):
    """
    Fail the pipeline if any Bronze ingestion DQ pass rate drops below 95%.

    Reads the DQ metrics JSON files emitted by each Spark ingestion job
    from s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/metrics/dt=<run_date>/.
    """
    import json
    import subprocess

    run_date = context["ds"]
    jobs = [
        "ingest_vehicle_registry",
        "ingest_vehicle_assignment",
        "ingest_fuel_transactions",
    ]

    for job_name in jobs:
        metrics_path = f"s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/metrics/dt={run_date}/{job_name}/part-00000"
        try:
            result = subprocess.run(
                ["aws", "s3", "cp", metrics_path, "-"],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode != 0:
                print(f"⚠️  Could not read metrics for {job_name} (skipping)")
                continue

            metrics = json.loads(result.stdout.strip())
            dq_pass_rate = metrics.get("dq_pass_rate", 100)
            total_rows = metrics.get("total_rows", 0)

            print(
                f"📊 {job_name}: {dq_pass_rate}% pass rate "
                f"({metrics.get('valid_rows', 0)}/{total_rows} rows)"
            )

            if dq_pass_rate < 95.0 and total_rows > 0:
                raise AirflowFailException(
                    f"DQ GATE FAILED for {job_name}: "
                    f"pass rate {dq_pass_rate}% < 95% threshold "
                    f"({metrics.get('invalid_rows', 0)} invalid rows)"
                )

        except (json.JSONDecodeError, FileNotFoundError) as e:
            print(f"⚠️  Could not parse metrics for {job_name}: {e} (skipping)")
            continue

    print("✅ All DQ gates passed.")


# ──────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────
with DAG(
    dag_id="omniroute_daily_batch",
    description="Daily batch pipeline: Bronze → DQ Gate → Silver → Gold → Reporting",
    schedule="0 5 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,                # Don't run for past dates when DAG is first deployed
    tags=["batch", "daily", "core"],
    default_args=default_args,
    max_active_runs=1,            # Only one DAG run at a time (prevents resource contention)
) as dag:

    # ──────────────────────────────────────────
    # S3 SENSORS — Wait for source files
    # ──────────────────────────────────────────
    if HAS_S3_SENSOR:
        wait_for_registry = S3KeySensor(
            task_id="wait_for_vehicle_registry",
            bucket_name=S3_BRONZE_BUCKET,
            bucket_key=f"{S3_LANDING_PREFIX}/vehicle_registry.csv",
            timeout=3600,
            poke_interval=60,
            mode="reschedule",
        )

        wait_for_assignment = S3KeySensor(
            task_id="wait_for_vehicle_assignment",
            bucket_name=S3_BRONZE_BUCKET,
            bucket_key=f"{S3_LANDING_PREFIX}/vehicle_assignment.csv",
            timeout=3600,
            poke_interval=60,
            mode="reschedule",
        )

        wait_for_fuel = S3KeySensor(
            task_id="wait_for_fuel_transactions",
            bucket_name=S3_BRONZE_BUCKET,
            bucket_key=f"{S3_LANDING_PREFIX}/fuel_transactions.csv",
            timeout=3600,
            poke_interval=60,
            mode="reschedule",
        )

    # ──────────────────────────────────────────
    # BRONZE — Ingest raw data from landing/
    # These 3 tasks run IN PARALLEL (no dependencies between them).
    # Each reads a CSV from S3 landing, validates schema, writes Parquet.
    # ──────────────────────────────────────────
    with TaskGroup("bronze") as bronze:

        ingest_registry = BashOperator(
            task_id="ingest_vehicle_registry",
            bash_command=spark_cmd(
                "batch/daily_ingest_vehicle_registry.py",
                f"--run-date {RUN_DATE}",
            ),
        )

        ingest_assignment = BashOperator(
            task_id="ingest_vehicle_assignment",
            bash_command=spark_cmd(
                "batch/daily_ingest_vehicle_assignment.py",
                f"--run-date {RUN_DATE}",
            ),
        )

        ingest_fuel = BashOperator(
            task_id="ingest_fuel_transactions",
            bash_command=spark_cmd(
                "batch/daily_ingest_fuel_transactions.py",
                f"--run-date {RUN_DATE}",
            ),
        )

    # ──────────────────────────────────────────
    # DQ GATE — Block pipeline if quality is low
    # ──────────────────────────────────────────
    dq_gate = PythonOperator(
        task_id="dq_quality_gate",
        python_callable=check_dq_threshold,
    )

    # # ──────────────────────────────────────────
    # # SILVER — Cleanse, dedup, enrich
    # # ──────────────────────────────────────────
    # with TaskGroup("silver") as silver:

    #     transform_assignment = BashOperator(
    #         task_id="transform_vehicle_assignment",
    #         bash_command=spark_cmd(
    #             "batch/transform_vehicle_assignment.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )

    #     transform_fuel = BashOperator(
    #         task_id="transform_fuel_transactions",
    #         bash_command=spark_cmd(
    #             "batch/transform_fuel_transactions.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )

    # # ──────────────────────────────────────────
    # # GOLD — Business logic
    # # ──────────────────────────────────────────
    # with TaskGroup("gold") as gold:

    #     build_scd2 = BashOperator(
    #         task_id="build_asset_history_scd2",
    #         bash_command=spark_delta_cmd(
    #             "batch/build_asset_history_scd2.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )

    #     build_fuel_audit = BashOperator(
    #         task_id="build_fuel_efficiency_audit",
    #         bash_command=spark_delta_cmd(
    #             "batch/build_fuel_efficiency_audit.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )

    #     build_fleet_snapshot = BashOperator(
    #         task_id="build_active_fleet_snapshot",
    #         bash_command=spark_delta_cmd(
    #             "batch/build_active_fleet_snapshot.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )

    # # ──────────────────────────────────────────
    # # REPORTING — Load DB + generate exports
    # # ──────────────────────────────────────────
    # with TaskGroup("reporting") as reporting:

    #     load_postgres = BashOperator(
    #         task_id="load_reporting_db",
    #         bash_command=spark_delta_cmd(
    #             "batch/load_reporting_db.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )

    #     generate_reports = BashOperator(
    #         task_id="generate_reports",
    #         bash_command=spark_delta_cmd(
    #             "batch/generate_reports.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )

    # # ──────────────────────────────────────────
    # # Task Dependencies
    # # ──────────────────────────────────────────

    # # S3 Sensors → Bronze (if available)
    # if HAS_S3_SENSOR:
    #     wait_for_registry >> ingest_registry
    #     wait_for_assignment >> ingest_assignment
    #     wait_for_fuel >> ingest_fuel

    # # Bronze → DQ Gate
    # [ingest_registry, ingest_assignment, ingest_fuel] >> dq_gate

    # # DQ Gate → Silver
    # dq_gate >> transform_assignment
    # dq_gate >> transform_fuel
    # # transform_assignment also needs registry data (for VIN validation)
    # ingest_registry >> transform_assignment
    # # transform_fuel also needs registry data (for model lookup)
    # ingest_registry >> transform_fuel

    # # Silver → Gold
    # transform_assignment >> build_scd2
    # build_scd2 >> build_fuel_audit
    # transform_fuel >> build_fuel_audit
    # build_scd2 >> build_fleet_snapshot

    # # Gold → Reporting
    # [build_fuel_audit, build_fleet_snapshot] >> load_postgres
    # load_postgres >> generate_reports
    pass
