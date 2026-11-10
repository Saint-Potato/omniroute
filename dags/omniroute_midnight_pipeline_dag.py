"""
OmniRoute — Midnight Pipeline DAG (Unified Daily + Monthly)
=============================================================
Schedule: Daily @ 00:00 UTC

Handles:
  - Yearly: Maintenance schedules + dim_date (with Variable-based retry)
  - Daily: Bronze (registry + assignment) → Safety Snapshot → Silver → Gold
  - Monthly: Rate deduction report + safety strikes reset (with Variable-based month gating)

DAG Dependency Graph:
═══════════════════════
  start
    │
    ▼
  check_yearly_needed ─────────────────────────────────┐
    │                                                  │
  [yearly needed]                              [skip yearly]
    │                                                  │
    ▼                                                  │
  yearly_bronze_ingest                                 │
    │                                                  │
    ├──► silver_maintenance                            │
    ├──► silver_dim_date                               │
    │                                                  │
    ▼                                                  │
  mark_yearly_done                                     │
    │                                                  │
    └───────────────────────────► yearly_gate ◄────────┘
                                    │
                                    ▼
                          daily_bronze_midnight
                                    │
                                    ▼
                          daily_safety_snapshot
                                    │
                        ┌───────────┴───────────────┐
                        │                           │
                        ▼                           ▼
              silver_vehicle_registry    check_monthly_needed ────────┐
                        │                       │                    │
                        ▼                 [monthly needed]     [skip monthly]
              silver_vehicle_assignment         │                    │
                        │                       ▼                    │
                        ▼             monthly_rate_deduction         │
              gold_active_fleet_snapshot        │                    │
                        │                       ▼                    │
                        │             safety_strikes_reset           │
                        │                       │                    │
                        │                       ▼                    │
                        │             mark_monthly_done              │
                        │                       │                    │
                        │                       └──► monthly_gate ◄──┘
                        │                               │
                        └──────────► end ◄──────────────┘

Yearly Retry Logic:
  Uses Airflow Variable `omniroute_yearly_done_YYYY`.
  If yearly tasks fail on Jan 1st, they retry daily until success.
  Once successful, the Variable is set to 'true' and yearly tasks
  are skipped for the rest of the year.

Monthly Retry Logic:
  Uses Airflow Variable `omniroute_monthly_done_YYYY_MM`.
  On the first run of a new month, the Variable is absent → monthly
  jobs run. Once successful, the Variable is set to 'true' and
  monthly tasks are skipped for the rest of that month.
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


# ──────────────────────────────────────────────────────────────
# Configuration — Airflow Variable `omniroute_config`
# (contents of config/s3_paths.json, read once per DAG-file parse)
# ──────────────────────────────────────────────────────────────
CONFIG = Variable.get("omniroute_config", deserialize_json=True)

AWS_CONN_ID = "aws_default"
RUN_DATE = "{{ ds }}"
RUN_MONTH = "{{ logical_date.strftime('%Y-%m') }}"


def s3_path(layer, name):
    """Full S3 prefix, ending in '/', for layers.<layer>.paths.<name>."""
    cfg = CONFIG["layers"][layer]
    return f"s3://{cfg['bucket']}/{cfg['prefix']}/{cfg['paths'][name]}/"


# ── S3 paths used by this DAG ──
BRONZE_ZONE_ARGS = {
    "--landing_path": s3_path("bronze", "landing"),
    "--ingested_path": s3_path("bronze", "ingested"),
    "--quarantine_path": s3_path("bronze", "quarantine"),
    "--archive_path": s3_path("bronze", "archive"),
}
BRONZE_INGESTED_PATH = BRONZE_ZONE_ARGS["--ingested_path"]

SILVER_VEHICLE_REGISTRY_PATH = s3_path("silver", "vehicle_registry")
SILVER_VEHICLE_ASSIGNMENT_PATH = s3_path("silver", "vehicle_assignment")
SILVER_MAINTENANCE_PATH = s3_path("silver", "dim_maintenance")
SILVER_DATE_PATH = s3_path("silver", "dim_date")

GOLD_FLEET_SNAPSHOT_PATH = s3_path("gold", "active_fleet_snapshot")
GOLD_DAILY_SAFETY_SNAPSHOT_PATH = s3_path("gold", "daily_safety_snapshot")
GOLD_MONTHLY_RATE_DEDUCTION_PATH = s3_path("gold", "monthly_rate_deduction")

# PostgreSQL connection, rendered from Airflow Variables at run time
PG_ARGS = {
    "--pg_host": "{{ var.value.pg_host }}",
    "--pg_port": "{{ var.value.get('pg_port', '5432') }}",
    "--pg_database": "{{ var.value.pg_database }}",
    "--pg_user": "{{ var.value.pg_user }}",
    "--pg_password": "{{ var.value.pg_password }}",
}


# ──────────────────────────────────────────────────────────────
# Helper — GlueJobOperator for glue.jobs.<job> (defaults + overrides)
# ──────────────────────────────────────────────────────────────
def build_glue_task(task_id, job, script_args, **operator_kwargs):
    """GlueJobOperator for glue.jobs.<job>; extra kwargs (e.g. retry_delay) go to the operator."""
    glue = CONFIG["glue"]
    cfg = {**glue["defaults"], **glue["jobs"][job]}
    return GlueJobOperator(
        task_id=task_id,
        job_name=cfg["job_name"],
        iam_role_name=cfg["iam_role_name"],
        region_name=CONFIG["aws_region"],
        aws_conn_id=AWS_CONN_ID,
        concurrent_run_limit=cfg["max_concurrent_runs"],
        wait_for_completion=True,
        verbose=True,
        script_args=script_args,
        create_job_kwargs={
            "GlueVersion": cfg["glue_version"],
            "NumberOfWorkers": cfg["number_of_workers"],
            "WorkerType": cfg["worker_type"],
            "Timeout": cfg["timeout_minutes"],
            "MaxRetries": cfg["max_retries"],
            "MaxCapacity": None,
            "Command": {
                "Name": "glueetl",
                "ScriptLocation": CONFIG["artifacts"]["glue_scripts"][cfg["layer"]] + cfg["script"],
                "PythonVersion": "3",
            },
            "DefaultArguments": dict(cfg["default_arguments"]),
        },
        **operator_kwargs,
    )


# ──────────────────────────────────────────────────────────────
# Yearly Check Functions
# ──────────────────────────────────────────────────────────────
def check_yearly_needed(**context):
    """
    Decide whether yearly tasks (maintenance + dim_date) need to run.
    Uses Airflow Variable `omniroute_yearly_done_YYYY`.
    """
    execution_date = context["logical_date"]
    year = execution_date.year
    var_name = f"omniroute_yearly_done_{year}"

    yearly_done = Variable.get(var_name, default="false")
    print(f"[yearly_check] Variable '{var_name}' = '{yearly_done}'")

    if yearly_done == "true":
        print(f"[yearly_check] Yearly tasks already completed for {year}. Skipping.")
        return "skip_yearly"
    else:
        print(f"[yearly_check] Yearly tasks NOT yet done for {year}. Running.")
        return "yearly_bronze_ingest"


def mark_yearly_done(**context):
    """Set the Airflow Variable to mark yearly tasks as completed."""
    execution_date = context["logical_date"]
    year = execution_date.year
    var_name = f"omniroute_yearly_done_{year}"
    Variable.set(var_name, "true")
    print(f"[yearly_check] ✓ Set '{var_name}' = 'true'. Yearly tasks won't run again in {year}.")


# ──────────────────────────────────────────────────────────────
# Monthly Check Functions
# ──────────────────────────────────────────────────────────────
def check_monthly_needed(**context):
    """
    Decide whether monthly tasks (rate deduction + strikes reset) need to run.
    Uses Airflow Variable `omniroute_monthly_done_YYYY_MM`.
    On the first run of a new month the variable is absent → run monthly jobs.
    Once successful the variable is set to 'true' → skip for the rest of the month.
    """
    execution_date = context["logical_date"]
    month_key = execution_date.strftime("%Y_%m")
    var_name = f"omniroute_monthly_done_{month_key}"

    monthly_done = Variable.get(var_name, default="false")
    print(f"[monthly_check] Variable '{var_name}' = '{monthly_done}'")

    if monthly_done == "true":
        print(f"[monthly_check] Monthly tasks already completed for {month_key}. Skipping.")
        return "skip_monthly"
    else:
        print(f"[monthly_check] Monthly tasks NOT yet done for {month_key}. Running.")
        return "monthly_rate_deduction_report"


def mark_monthly_done(**context):
    """Set the Airflow Variable to mark monthly tasks as completed for this month."""
    execution_date = context["logical_date"]
    month_key = execution_date.strftime("%Y_%m")
    var_name = f"omniroute_monthly_done_{month_key}"
    Variable.set(var_name, "true")
    print(f"[monthly_check] ✓ Set '{var_name}' = 'true'. Monthly tasks won't run again in {month_key}.")


# ──────────────────────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "omniroute",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="omniroute_midnight_pipeline",
    description=(
        "Unified midnight pipeline: Yearly (maintenance/dim_date) → "
        "Daily Bronze (registry+assignment) → Safety Snapshot → Silver → "
        "Gold fleet snapshot + Monthly (rate deduction/strikes reset) with "
        "Variable-based gating."
    ),
    schedule="0 0 * * *",              # Daily at 00:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "silver", "gold", "glue", "delta", "daily", "yearly", "monthly", "midnight"],
    default_args=default_args,
    max_active_runs=1,
) as dag:

    # ── Markers ──
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    # ══════════════════════════════════════════════════════════
    # YEARLY BRANCH — maintenance schedules + dim_date
    # ══════════════════════════════════════════════════════════
    check_yearly = BranchPythonOperator(
        task_id="check_yearly_needed",
        python_callable=check_yearly_needed,
    )

    skip_yearly = EmptyOperator(task_id="skip_yearly")

    # ── Yearly Bronze: Ingest maintenance_schedules.csv ──
    yearly_bronze = build_glue_task(
        task_id="yearly_bronze_ingest",
        job="bronze_ingestion_yearly",
        script_args={"--run_date": RUN_DATE, **BRONZE_ZONE_ARGS},
    )

    silver_maintenance = build_glue_task(
        task_id="silver_maintenance_schedules",
        job="silver_maintenance_schedules",
        script_args={
            "--run_date": RUN_DATE,
            "--bronze_ingested_path": BRONZE_INGESTED_PATH,
            "--silver_output_path": SILVER_MAINTENANCE_PATH,
        },
    )

    silver_dim_date = build_glue_task(
        task_id="silver_dim_date",
        job="silver_dim_date",
        script_args={
            "--year": "{{ logical_date.year }}",
            "--silver_output_path": SILVER_DATE_PATH,
        },
    )

    mark_done = PythonOperator(
        task_id="mark_yearly_done",
        python_callable=mark_yearly_done,
    )

    yearly_gate = EmptyOperator(
        task_id="yearly_gate",
        trigger_rule="none_failed_min_one_success",
    )

    # ══════════════════════════════════════════════════════════
    # DAILY — Bronze (registry + assignment) → Safety Snapshot
    # ══════════════════════════════════════════════════════════
    daily_bronze_midnight = build_glue_task(
        task_id="daily_bronze_midnight",
        job="bronze_ingestion",
        script_args={
            "--run_date": RUN_DATE,
            **BRONZE_ZONE_ARGS,
            "--datasets": "vehicle_registry,vehicle_assignment",
        },
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    daily_safety_snapshot = build_glue_task(
        task_id="daily_safety_snapshot",
        job="daily_safety_snapshot",
        script_args={
            "--run_date": RUN_DATE,
            "--silver_assignment_path": SILVER_VEHICLE_ASSIGNMENT_PATH,
            "--gold_output_path": GOLD_DAILY_SAFETY_SNAPSHOT_PATH,
            **PG_ARGS,
        },
    )

    # ══════════════════════════════════════════════════════════
    # BRANCH 1 — Silver vehicle registry → assignment → Gold
    # ══════════════════════════════════════════════════════════
    silver_registry = build_glue_task(
        task_id="silver_vehicle_registry",
        job="silver_vehicle_registry",
        script_args={
            "--run_date": RUN_DATE,
            "--bronze_ingested_path": BRONZE_INGESTED_PATH,
            "--silver_output_path": SILVER_VEHICLE_REGISTRY_PATH,
        },
    )

    silver_assignment = build_glue_task(
        task_id="silver_vehicle_assignment",
        job="silver_vehicle_assignment",
        script_args={
            "--run_date": RUN_DATE,
            "--bronze_ingested_path": BRONZE_INGESTED_PATH,
            "--silver_output_path": SILVER_VEHICLE_ASSIGNMENT_PATH,
            "--silver_vehicle_path": SILVER_VEHICLE_REGISTRY_PATH,
            "--silver_maintenance_path": SILVER_MAINTENANCE_PATH,
            **PG_ARGS,
        },
    )

    gold_fleet_snapshot = build_glue_task(
        task_id="gold_active_fleet_snapshot",
        job="gold_active_fleet_snapshot",
        script_args={
            "--run_date": RUN_DATE,
            "--silver_assignment_path": SILVER_VEHICLE_ASSIGNMENT_PATH,
            "--silver_vehicle_path": SILVER_VEHICLE_REGISTRY_PATH,
            "--gold_output_path": GOLD_FLEET_SNAPSHOT_PATH,
        },
    )

    # ══════════════════════════════════════════════════════════
    # BRANCH 2 — Monthly: rate deduction → strikes reset
    # ══════════════════════════════════════════════════════════
    check_monthly = BranchPythonOperator(
        task_id="check_monthly_needed",
        python_callable=check_monthly_needed,
    )

    skip_monthly = EmptyOperator(task_id="skip_monthly")

    monthly_rate_deduction = build_glue_task(
        task_id="monthly_rate_deduction_report",
        job="monthly_rate_deduction_report",
        script_args={
            "--run_month": RUN_MONTH,
            "--run_date": RUN_DATE,
            "--gold_snapshot_path": GOLD_DAILY_SAFETY_SNAPSHOT_PATH,
            "--gold_output_path": GOLD_MONTHLY_RATE_DEDUCTION_PATH,
        },
    )

    safety_strikes_reset = build_glue_task(
        task_id="safety_strikes_reset",
        job="monthly_safety_strikes_reset",
        script_args={
            "--run_month": RUN_MONTH,
            "--run_date": RUN_DATE,
            **PG_ARGS,
        },
    )

    mark_monthly = PythonOperator(
        task_id="mark_monthly_done",
        python_callable=mark_monthly_done,
    )

    monthly_gate = EmptyOperator(
        task_id="monthly_gate",
        trigger_rule="none_failed_min_one_success",
    )

    # ══════════════════════════════════════════════════════════
    # TASK DEPENDENCIES
    # ══════════════════════════════════════════════════════════

    # ── Yearly branch ──
    start >> check_yearly
    check_yearly >> yearly_bronze >> [silver_maintenance, silver_dim_date] >> mark_done >> yearly_gate
    check_yearly >> skip_yearly >> yearly_gate

    # ── Daily path: yearly gate must pass before daily work begins ──
    yearly_gate >> daily_bronze_midnight >> daily_safety_snapshot

    # ── Branch 1: Silver → Gold fleet snapshot ──
    daily_safety_snapshot >> silver_registry >> silver_assignment >> gold_fleet_snapshot

    # ── Branch 2: Monthly (Variable-gated) ──
    daily_safety_snapshot >> check_monthly
    check_monthly >> monthly_rate_deduction >> safety_strikes_reset >> mark_monthly >> monthly_gate
    check_monthly >> skip_monthly >> monthly_gate

    # ── End waits for both branches ──
    [gold_fleet_snapshot, monthly_gate] >> end
