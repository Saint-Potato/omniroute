"""
OmniRoute Yearly Maintenance DAG
===================================
Schedule: Jan 1st @ 00:00 UTC
Pipeline: Bronze → Silver

Ingests maintenance_schedules.csv from landing zone,
then cleans and deduplicates for Silver layer.

DAG Dependency Graph:
═══════════════════
Bronze:
  ingest_maintenance_schedules

Silver:
  ingest_maintenance_schedules >> transform_maintenance_schedules

Note: The Silver maintenance table is consumed by the DAILY DAG's
transform_fuel_transactions task (LEFT JOIN to flag is_maintenance_day).
So this yearly DAG must run at least once before daily fuel processing
can correctly flag maintenance days.
"""

from datetime import datetime, timedelta

# Airflow 3.x imports — core authoring objects moved to airflow.sdk,
# operators moved to airflow.providers.standard
from airflow.sdk import DAG, TaskGroup
from airflow.providers.standard.operators.bash import BashOperator


# ──────────────────────────────────────────────
# DAG Configuration
# ──────────────────────────────────────────────
SPARK_SUBMIT = "spark-submit"

# Delta Lake package — required for Silver layer writes.
DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.3.0"

JOBS_DIR = "/opt/omniroute/spark_jobs"
RUN_DATE = "{{ ds }}"  # YYYY-MM-DD execution date

default_args = {
    "owner": "omniroute",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ──────────────────────────────────────────────
# Helpers — build spark-submit commands
# ──────────────────────────────────────────────
def spark_cmd(script: str, extra_args: str = "") -> str:
    """Build a spark-submit command for Bronze layer (plain Parquet, no Delta)."""
    return f"{SPARK_SUBMIT} {JOBS_DIR}/{script} {extra_args}".strip()


def spark_delta_cmd(script: str, extra_args: str = "") -> str:
    """Build a spark-submit command for Silver/Gold layer (requires Delta Lake package)."""
    return (
        f"{SPARK_SUBMIT} "
        f"--packages {DELTA_PACKAGE} "
        f"{JOBS_DIR}/{script} {extra_args}"
    ).strip()


# ──────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────
with DAG(
    dag_id="omniroute_yearly_maintenance",
    description="Yearly maintenance: ingest and clean maintenance schedules",
    schedule="0 0 1 1 *",        # January 1st at midnight UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["batch", "yearly", "maintenance"],
    default_args=default_args,
    max_active_runs=1,
) as dag:

    # ──────────────────────────────────────────
    # BRONZE — Ingest maintenance_schedules.csv
    # Reads raw CSV from S3 landing, validates schema, writes Parquet.
    # ──────────────────────────────────────────
    with TaskGroup("bronze") as bronze:

        ingest_maintenance = BashOperator(
            task_id="ingest_maintenance_schedules",
            bash_command=spark_cmd(
                "batch/yearly_ingest_maintenance_schedules.py",
                f"--run-date {RUN_DATE}",
            ),
        )

    # ──────────────────────────────────────────
    # SILVER — Clean and deduplicate maintenance schedules
    # Parses dates, removes INVALID_ VINs and unparseable dates,
    # deduplicates by (vin, service_date), writes Delta Lake.
    #
    # This Silver table is then consumed by daily fuel transform
    # (LEFT JOIN to flag is_maintenance_day for fuel audit exclusion).
    # ──────────────────────────────────────────
    with TaskGroup("silver") as silver:

        transform_maintenance = BashOperator(
            task_id="transform_maintenance_schedules",
            bash_command=spark_delta_cmd(
                "batch/transform_maintenance_schedules.py",
                f"--run-date {RUN_DATE}",
            ),
        )

    # ──────────────────────────────────────────
    # Task Dependencies
    # ──────────────────────────────────────────

    # Bronze → Silver: maintenance must be ingested before transformation
    ingest_maintenance >> transform_maintenance
