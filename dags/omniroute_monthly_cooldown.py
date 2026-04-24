"""
OmniRoute Monthly Cooldown DAG
================================
Schedule: 1st of month @ 05:00 UTC
Pipeline: Gold → Reporting

Resets driver strikes for eligible (non-suspended) drivers
and generates the monthly rate deduction report.

Business Logic:
    - Drivers with status != 'SUSPENDED' get strike_count reset to 0
    - current_adjusted_rate restored to base_rate
    - Suspended drivers are EXCLUDED from cooldown (strikes persist)
    - Generates TXT report with deductions for the closed month
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
    dag_id="omniroute_monthly_cooldown",
    description="Monthly cooldown: reset driver strikes, generate rate deduction report",
    schedule="0 5 1 * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["batch", "monthly", "safety"],
    default_args=default_args,
    max_active_runs=1,
) as dag:

    # # ──────────────────────────────────────────
    # # GOLD — Reset driver strikes
    # # ──────────────────────────────────────────
    # with TaskGroup("gold") as gold:

    #     reset_strikes = BashOperator(
    #         task_id="reset_driver_strikes",
    #         bash_command=spark_cmd(
    #             "batch/reset_driver_strikes.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )

    # # ──────────────────────────────────────────
    # # REPORTING — Monthly rate deduction report
    # # ──────────────────────────────────────────
    # with TaskGroup("reporting") as reporting:

    #     generate_report = BashOperator(
    #         task_id="generate_rate_deduction_report",
    #         bash_command=spark_cmd(
    #             "batch/generate_rate_deduction_report.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )

    # # ──────────────────────────────────────────
    # # Task Dependencies
    # # ──────────────────────────────────────────

    # # Gold → Reporting
    # reset_strikes >> generate_report
    pass