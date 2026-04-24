"""
OmniRoute Yearly Maintenance DAG
===================================
Schedule: Jan 1st @ 00:00 UTC
Pipeline: Bronze → Silver

Ingests maintenance_schedules.csv from landing zone,
then cleans and deduplicates for Silver layer.
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

default_args = {
    "owner": "omniroute",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
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
                "batch/yearly_ingest_maintenance_schedules.py",
                f"--run-date {RUN_DATE}",
            ),
        )

    # ──────────────────────────────────────────
    # SILVER — Clean and deduplicate
    # ──────────────────────────────────────────
    # with TaskGroup("silver") as silver:
    #
    #     clean_maintenance = BashOperator(
    #         task_id="clean_maintenance_logs",
    #         bash_command=spark_cmd(
    #             "batch/clean_maintenance_logs.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )

    # ──────────────────────────────────────────
    # Task Dependencies
    # ──────────────────────────────────────────

    # Bronze → Silver
    # bronze >> silver
    pass
