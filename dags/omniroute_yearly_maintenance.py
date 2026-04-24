"""
OmniRoute Yearly Maintenance DAG
===================================
Schedule: Jan 1st @ 00:00 UTC
Pipeline: Bronze → Silver

Ingests maintenance_schedules.csv from landing zone,
then cleans and deduplicates for Silver layer.

Business Logic:
    - Maintenance schedules define planned vehicle downtime
    - Used as a lookup by the fuel efficiency audit to EXCLUDE
      days when a vehicle was under maintenance from audit scoring
    - Deduplication key: (vin, service_date)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


# ──────────────────────────────────────────────
# DAG Configuration
# ──────────────────────────────────────────────
SPARK_SUBMIT = "spark-submit"
JOBS_DIR = "/opt/omniroute/spark_jobs"
RUN_DATE = "{{ ds }}"  # YYYY-MM-DD execution date


def on_failure_callback(context):
    """Log failure details for alerting integration."""
    ti = context["task_instance"]
    dag_id = context["dag"].dag_id
    print(
        f"🚨 FAILURE: {dag_id}.{ti.task_id} | "
        f"execution_date={context['execution_date']} | "
        f"log_url={ti.log_url}"
    )


default_args = {
    "owner": "omniroute",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "on_failure_callback": on_failure_callback,
    "sla": timedelta(hours=1),
}


# ──────────────────────────────────────────────
# Helper — build spark-submit command
# ──────────────────────────────────────────────
def spark_cmd(script: str, extra_args: str = "") -> str:
    """Build a spark-submit command string."""
    return f"{SPARK_SUBMIT} {JOBS_DIR}/{script} {extra_args}".strip()


# ──────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────
with DAG(
    dag_id="omniroute_yearly_maintenance",
    description="Yearly maintenance: ingest and clean maintenance schedules",
    schedule="0 0 1 1 *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["batch", "yearly", "maintenance"],
    default_args=default_args,
    max_active_runs=1,
) as dag:

    # ──────────────────────────────────────────
    # BRONZE — Ingest maintenance_schedules.csv
    # ──────────────────────────────────────────
    with TaskGroup("bronze") as bronze:

        ingest_maintenance = BashOperator(
            task_id="ingest_maintenance_schedules",
            bash_command=spark_cmd(
                "batch/ingest_maintenance_schedules.py",
                f"--run-date {RUN_DATE}",
            ),
        )

    # # ──────────────────────────────────────────
    # # SILVER — Clean and deduplicate
    # # ──────────────────────────────────────────
    # with TaskGroup("silver") as silver:

    #     clean_maintenance = BashOperator(
    #         task_id="clean_maintenance_logs",
    #         bash_command=spark_cmd(
    #             "batch/clean_maintenance_logs.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )

    # # ──────────────────────────────────────────
    # # Task Dependencies
    # # ──────────────────────────────────────────

    # # Bronze → Silver
    # ingest_maintenance >> clean_maintenance
    pass