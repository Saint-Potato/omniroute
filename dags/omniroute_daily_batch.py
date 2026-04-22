"""
OmniRoute Daily Batch DAG
==========================
Schedule: Daily @ 05:00 UTC
Pipeline: Bronze → Silver → Gold → Reporting

Ingests vehicle registry, vehicle assignment, and fuel transactions,
then transforms, builds business tables (SCD2, fuel audit, fleet snapshot),
loads reporting DB, and generates CSV reports.
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
    dag_id="omniroute_daily_batch",
    description="Daily batch pipeline: Bronze → Silver → Gold → Reporting",
    schedule="0 5 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["batch", "daily", "core"],
    default_args=default_args,
    max_active_runs=1,
) as dag:

    # ──────────────────────────────────────────
    # BRONZE — Ingest raw data from landing/
    # ──────────────────────────────────────────
    with TaskGroup("bronze") as bronze:

        ingest_registry = BashOperator(
            task_id="ingest_vehicle_registry",
            bash_command=spark_cmd(
                "batch/ingest_vehicle_registry.py",
                f"--run-date {RUN_DATE}",
            ),
        )

        ingest_assignment = BashOperator(
            task_id="ingest_vehicle_assignment",
            bash_command=spark_cmd(
                "batch/ingest_vehicle_assignment.py",
                f"--run-date {RUN_DATE}",
            ),
        )

        ingest_fuel = BashOperator(
            task_id="ingest_fuel_transactions",
            bash_command=spark_cmd(
                "batch/ingest_fuel_transactions.py",
                f"--run-date {RUN_DATE}",
            ),
        )

    # ──────────────────────────────────────────
    # SILVER — Cleanse, dedup, enrich
    # ──────────────────────────────────────────
    # with TaskGroup("silver") as silver:
    #
    #     transform_assignment = BashOperator(
    #         task_id="transform_vehicle_assignment",
    #         bash_command=spark_cmd(
    #             "batch/transform_vehicle_assignment.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )
    #
    #     transform_fuel = BashOperator(
    #         task_id="transform_fuel_transactions",
    #         bash_command=spark_cmd(
    #             "batch/transform_fuel_transactions.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )

    # ──────────────────────────────────────────
    # GOLD — Business logic
    # ──────────────────────────────────────────
    # with TaskGroup("gold") as gold:
    #
    #     build_scd2 = BashOperator(
    #         task_id="build_asset_history_scd2",
    #         bash_command=spark_cmd(
    #             "batch/build_asset_history_scd2.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )
    #
    #     build_fuel_audit = BashOperator(
    #         task_id="build_fuel_efficiency_audit",
    #         bash_command=spark_cmd(
    #             "batch/build_fuel_efficiency_audit.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )
    #
    #     build_fleet_snapshot = BashOperator(
    #         task_id="build_active_fleet_snapshot",
    #         bash_command=spark_cmd(
    #             "batch/build_active_fleet_snapshot.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )

    # ──────────────────────────────────────────
    # REPORTING — Load DB + generate exports
    # ──────────────────────────────────────────
    # with TaskGroup("reporting") as reporting:
    #
    #     load_postgres = BashOperator(
    #         task_id="load_reporting_db",
    #         bash_command=spark_cmd(
    #             "batch/load_reporting_db.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )
    #
    #     generate_reports = BashOperator(
    #         task_id="generate_reports",
    #         bash_command=spark_cmd(
    #             "batch/generate_reports.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )

    # ──────────────────────────────────────────
    # Task Dependencies
    # ──────────────────────────────────────────

    # Bronze → Silver
    # [ingest_registry, ingest_assignment] >> transform_assignment
    # [ingest_registry, ingest_fuel] >> transform_fuel

    # Silver → Gold
    # transform_assignment >> build_scd2
    # build_scd2 >> build_fuel_audit
    # transform_fuel >> build_fuel_audit
    # build_scd2 >> build_fleet_snapshot

    # Gold → Reporting
    # [build_fuel_audit, build_fleet_snapshot] >> load_postgres
    # load_postgres >> generate_reports
    pass
