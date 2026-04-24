"""
OmniRoute Daily Batch DAG
==========================
Schedule: Daily @ 05:00 UTC
Pipeline: Bronze → Silver → Gold → Reporting

Ingests vehicle registry, vehicle assignment, and fuel transactions,
then transforms, builds business tables (SCD2, fuel audit, fleet snapshot),
loads reporting DB, and generates CSV reports.

DAG Dependency Graph (what's wired so far):
═══════════════════════════════════════════
Bronze (parallel):
  ingest_vehicle_registry  ─┐
  ingest_vehicle_assignment ─┤
  ingest_fuel_transactions ──┘

Silver:
  [ingest_registry]                  >> transform_vehicle_registry
  [ingest_registry, ingest_assign]   >> transform_vehicle_assignment
  [ingest_registry, ingest_fuel]     >> transform_fuel_transactions

  ⚠ transform_fuel depends on Silver maintenance being available.
     Maintenance is loaded yearly, so it should already exist.

Gold (TODO — commented out, needs Spark jobs):
  transform_assignment >> build_scd2
  build_scd2 >> build_fuel_audit
  transform_fuel >> build_fuel_audit
  build_scd2 >> build_fleet_snapshot

Reporting (TODO — commented out):
  [fuel_audit, fleet_snapshot] >> load_postgres >> generate_reports
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

# Delta Lake package — required for all Silver layer writes.
# This is passed to every spark-submit command that touches Delta tables.
DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.3.0"

JOBS_DIR = "/opt/omniroute/spark_jobs"  # Deploy target: sudo cp -r spark_jobs/* /opt/omniroute/spark_jobs/

# Airflow Jinja template: {{ ds }} resolves to the DAG's logical execution date
# e.g., for a run triggered on 2026-04-24 at 05:00, ds = "2026-04-24"
# This ensures all Spark jobs process data for the SAME date consistently
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
# DAG Definition
# ──────────────────────────────────────────────
with DAG(
    dag_id="omniroute_daily_batch",
    description="Daily batch pipeline: Bronze → Silver → Gold → Reporting",
    schedule="0 5 * * *",        # Every day at 05:00 UTC
    start_date=datetime(2026, 4, 1),
    catchup=False,                # Don't run for past dates when DAG is first deployed
    tags=["batch", "daily", "core"],
    default_args=default_args,
    max_active_runs=1,            # Only one DAG run at a time (prevents resource contention)
) as dag:

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
    # SILVER — Cleanse, dedup, enrich
    # These tasks transform Bronze Parquet → Silver Delta Lake.
    # Each uses --packages to load the Delta Lake dependency.
    # ──────────────────────────────────────────
    with TaskGroup("silver") as silver:

        # Transform Vehicle Registry (Silver)
        # Dependency: needs Bronze registry to be ingested first.
        # Logic: dedup by VIN, validate fuel_type, TRIM/UPPER, write Delta.
        transform_registry = BashOperator(
            task_id="transform_vehicle_registry",
            bash_command=spark_delta_cmd(
                "batch/transform_vehicle_registry.py",
                f"--run-date {RUN_DATE}",
            ),
        )

        # Transform Vehicle Assignment (Silver)
        # Dependency: needs Bronze registry (for VIN validation) AND
        #             Bronze assignment to be ingested.
        # Logic: Unix→Date, ROW_NUMBER dedup (highest daily_rate wins),
        #        TRIM/UPPER region, write Delta.
        transform_assignment = BashOperator(
            task_id="transform_vehicle_assignment",
            bash_command=spark_delta_cmd(
                "batch/transform_vehicle_assignment.py",
                f"--run-date {RUN_DATE}",
            ),
        )

        # Transform Fuel Transactions (Silver)
        # Dependency: needs Bronze fuel AND Bronze registry ingested.
        #             Also JOINs with Silver maintenance (yearly, should exist).
        # Logic: dedup, timestamp parse, weekend flag, maintenance JOIN,
        #        LAG() for distance, km_per_liter calc, write Delta.
        transform_fuel = BashOperator(
            task_id="transform_fuel_transactions",
            bash_command=spark_delta_cmd(
                "batch/transform_fuel_transactions.py",
                f"--run-date {RUN_DATE}",
            ),
        )

    # ──────────────────────────────────────────
    # GOLD — Business logic (TODO: implement Spark jobs)
    # ──────────────────────────────────────────
    # with TaskGroup("gold") as gold:
    #
    #     build_scd2 = BashOperator(
    #         task_id="build_asset_history_scd2",
    #         bash_command=spark_delta_cmd(
    #             "batch/build_asset_history_scd2.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )
    #
    #     build_fuel_audit = BashOperator(
    #         task_id="build_fuel_efficiency_audit",
    #         bash_command=spark_delta_cmd(
    #             "batch/build_fuel_efficiency_audit.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )
    #
    #     build_fleet_snapshot = BashOperator(
    #         task_id="build_active_fleet_snapshot",
    #         bash_command=spark_delta_cmd(
    #             "batch/build_active_fleet_snapshot.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )

    # ──────────────────────────────────────────
    # REPORTING — Load DB + generate exports (TODO: implement)
    # ──────────────────────────────────────────
    # with TaskGroup("reporting") as reporting:
    #
    #     load_postgres = BashOperator(
    #         task_id="load_reporting_db",
    #         bash_command=spark_delta_cmd(
    #             "batch/load_reporting_db.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )
    #
    #     generate_reports = BashOperator(
    #         task_id="generate_reports",
    #         bash_command=spark_delta_cmd(
    #             "batch/generate_reports.py",
    #             f"--run-date {RUN_DATE}",
    #         ),
    #     )

    # ──────────────────────────────────────────
    # Task Dependencies
    # ──────────────────────────────────────────

    # Bronze → Silver
    # Registry must be ingested before its Silver transform can run
    ingest_registry >> transform_registry

    # Assignment transform needs BOTH registry AND assignment ingested
    # (registry provides VINs that assignment references)
    [ingest_registry, ingest_assignment] >> transform_assignment

    # Fuel transform needs BOTH registry AND fuel ingested
    # (registry provides VINs, fuel provides the raw transactions)
    [ingest_registry, ingest_fuel] >> transform_fuel

    # Silver → Gold (uncomment when Gold jobs are ready)
    # transform_assignment >> build_scd2
    # build_scd2 >> build_fuel_audit
    # transform_fuel >> build_fuel_audit
    # build_scd2 >> build_fleet_snapshot

    # Gold → Reporting (uncomment when Reporting jobs are ready)
    # [build_fuel_audit, build_fleet_snapshot] >> load_postgres
    # load_postgres >> generate_reports
