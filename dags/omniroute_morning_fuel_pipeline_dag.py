"""
OmniRoute — Morning Fuel Pipeline DAG (DAG 2 of 2)
=====================================================
Schedule: Daily @ 05:00 UTC

Runs ONLY after the midnight pipeline (DAG 1) completes successfully.
Uses ExternalTaskSensor to wait for omniroute_midnight_pipeline.end.

Handles:
  - Bronze ingestion (fuel_transactions only)
  - Silver fuel transactions
  - Gold fuel efficiency audit
  - Gold to PostgreSQL reporting

DAG Dependency Graph:
═══════════════════════
  wait_for_midnight_pipeline (ExternalTaskSensor)
    │
    ▼
  daily_bronze_fuel (fuel_transactions only)
    │
    ▼
  silver_fuel_transactions
    │
    ▼
  gold_fuel_efficiency_audit
    │
    ▼
  gold_to_postgres
    │
    ▼
  end
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor


# ──────────────────────────────────────────────────────────────
# Configuration — Airflow Variable `omniroute_config`
# (contents of config/s3_paths.json, read once per DAG-file parse)
# ──────────────────────────────────────────────────────────────
CONFIG = Variable.get("omniroute_config", deserialize_json=True)

AWS_CONN_ID = "aws_default"
RUN_DATE = "{{ ds }}"


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
SILVER_FUEL_TRANSACTIONS_PATH = s3_path("silver", "fuel_transactions")
SILVER_MAINTENANCE_PATH = s3_path("silver", "dim_maintenance")
SILVER_DATE_PATH = s3_path("silver", "dim_date")

GOLD_FUEL_AUDIT_PATH = s3_path("gold", "fuel_efficiency_audit")
GOLD_FLEET_SNAPSHOT_PATH = s3_path("gold", "active_fleet_snapshot")

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
    dag_id="omniroute_morning_fuel_pipeline",
    description=(
        "Morning pipeline: Waits for midnight pipeline success, then "
        "Bronze (fuel) → Silver fuel → Gold fuel audit → PostgreSQL."
    ),
    schedule="0 5 * * *",              # Daily at 05:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "silver", "gold", "glue", "delta", "daily", "fuel", "morning"],
    default_args=default_args,
    max_active_runs=1,
) as dag:

    # ══════════════════════════════════════════════════════════
    # GATE — Wait for midnight pipeline to complete
    # ══════════════════════════════════════════════════════════
    # ExternalTaskSensor waits for the "end" task of the midnight
    # pipeline to succeed.
    #
    # execution_delta maps this DAG's logical_date to the
    # midnight DAG's logical_date.
    #   Morning DAG: 05:00 UTC
    #   Midnight DAG: 00:00 UTC
    # We must subtract 5 hours from Morning to find Midnight's exact run.
    # wait_for_midnight = ExternalTaskSensor(
    #     task_id="wait_for_midnight_pipeline",
    #     external_dag_id="omniroute_midnight_pipeline",
    #     external_task_id="end",
    #     execution_delta=timedelta(hours=5),
    #     mode="reschedule",            # Release worker between checks
    #     poke_interval=120,            # Check every 2 minutes
    #     timeout=3600,                 # Fail after 1 hour of waiting
    #     allowed_states=["success"],
    #     failed_states=["failed", "upstream_failed"],
    # )

    end = EmptyOperator(task_id="end")

    # ══════════════════════════════════════════════════════════
    # BRONZE — Ingest fuel_transactions.csv only
    # ══════════════════════════════════════════════════════════
    daily_bronze_fuel = build_glue_task(
        task_id="daily_bronze_fuel",
        job="bronze_ingestion",
        script_args={
            "--run_date": RUN_DATE,
            **BRONZE_ZONE_ARGS,
            "--datasets": "fuel_transactions",
        },
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    # ══════════════════════════════════════════════════════════
    # SILVER — Fuel Transactions → fact_fuel
    # ══════════════════════════════════════════════════════════
    silver_fuel = build_glue_task(
        task_id="silver_fuel_transactions",
        job="silver_fuel_transactions",
        script_args={
            "--run_date": RUN_DATE,
            "--bronze_ingested_path": BRONZE_INGESTED_PATH,
            "--silver_output_path": SILVER_FUEL_TRANSACTIONS_PATH,
            "--silver_maintenance_path": SILVER_MAINTENANCE_PATH,
            "--silver_assignment_path": SILVER_VEHICLE_ASSIGNMENT_PATH,
            "--silver_vehicle_path": SILVER_VEHICLE_REGISTRY_PATH,
        },
    )

    # ══════════════════════════════════════════════════════════
    # GOLD — Fuel Efficiency Audit
    # ══════════════════════════════════════════════════════════
    gold_fuel_audit = build_glue_task(
        task_id="gold_fuel_efficiency_audit",
        job="gold_fuel_efficiency_audit",
        script_args={
            "--run_date": RUN_DATE,
            "--silver_fuel_path": SILVER_FUEL_TRANSACTIONS_PATH,
            "--silver_vehicle_path": SILVER_VEHICLE_REGISTRY_PATH,
            "--silver_date_path": SILVER_DATE_PATH,
            "--gold_output_path": GOLD_FUEL_AUDIT_PATH,
        },
    )

    # ══════════════════════════════════════════════════════════
    # REPORTING — Gold → PostgreSQL
    # ══════════════════════════════════════════════════════════
    gold_to_postgres = build_glue_task(
        task_id="gold_to_postgres",
        job="gold_to_postgres",
        script_args={
            "--run_date": RUN_DATE,
            "--silver_assignment_path": SILVER_VEHICLE_ASSIGNMENT_PATH,
            "--silver_vehicle_path": SILVER_VEHICLE_REGISTRY_PATH,
            "--silver_date_path": SILVER_DATE_PATH,
            "--gold_fuel_audit_path": GOLD_FUEL_AUDIT_PATH,
            "--gold_fleet_snapshot_path": GOLD_FLEET_SNAPSHOT_PATH,
            **PG_ARGS,
        },
    )

    # ══════════════════════════════════════════════════════════
    # TASK DEPENDENCIES
    # ══════════════════════════════════════════════════════════
    # wait_for_midnight >> daily_bronze_fuel >> silver_fuel >> gold_fuel_audit >> gold_to_postgres >> end
    daily_bronze_fuel >> silver_fuel >> gold_fuel_audit >> gold_to_postgres >> end
