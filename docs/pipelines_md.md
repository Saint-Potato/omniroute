# OmniRoute — Pipeline Documentation

Three Airflow DAGs orchestrate the OmniRoute data platform: a nightly batch pipeline,
a morning fuel pipeline that depends on it, and a continuously-running EMR streaming
pipeline for real-time ingestion.

---

## 1. Midnight Pipeline (`omniroute_midnight_pipeline`)

**Schedule:** Daily at `00:00 UTC`  
**Trigger:** Automatic (cron)  
**Max Active Runs:** 1

The core batch pipeline. Runs every night and handles three cadences of work — yearly,
daily, and monthly — in a single DAG using Airflow Variable-based gating to avoid
re-running work that has already succeeded.

### Dependency Graph

```
start
  │
  ▼
check_yearly_needed ────────────────────────────────┐
  │                                                 │
[yearly needed]                             [skip_yearly]
  │                                                 │
  ▼                                                 │
yearly_bronze_ingest                                │
  │                                                 │
  ├──► silver_maintenance_schedules                 │
  └──► silver_dim_date                              │
            │                                       │
            ▼                                       │
      mark_yearly_done                              │
            │                                       │
            └──────────────► yearly_gate ◄──────────┘
                                  │
                                  ▼
                        daily_bronze_midnight
                                  │
                                  ▼
                        daily_safety_snapshot
                                  │
                    ┌─────────────┴──────────────────┐
                    │                                │
                    ▼                                ▼
        silver_vehicle_registry         check_monthly_needed ─────────┐
                    │                         │                       │
                    ▼                   [monthly needed]        [skip_monthly]
        silver_vehicle_assignment             │                       │
                    │                         ▼                       │
                    ▼               monthly_rate_deduction            │
        gold_active_fleet_snapshot            │                       │
                    │                         ▼                       │
                    │               safety_strikes_reset              │
                    │                         │                       │
                    │                         ▼                       │
                    │                 mark_monthly_done               │
                    │                         │                       │
                    │                         └──► monthly_gate ◄─────┘
                    │                                   │
                    └──────────────► end ◄──────────────┘
```

### Yearly Tasks

Runs once per calendar year. Controlled by the Airflow Variable
`omniroute_yearly_done_YYYY` (e.g. `omniroute_yearly_done_2026`).

On each daily run, `check_yearly_needed` reads this variable. If it equals `"true"`,
the branch skips to `yearly_gate`. If absent or `"false"`, the yearly tasks execute.
Once they complete successfully, `mark_yearly_done` sets the variable to `"true"` and
they are skipped for the rest of the year.

This design means that if yearly tasks fail on January 1st, they will automatically
retry on January 2nd, 3rd, and so on until they succeed — no manual intervention needed.

| Task | Glue Job | Output |
|---|---|---|
| `yearly_bronze_ingest` | `bronze_ingest` | Bronze ingested path |
| `silver_maintenance_schedules` | `silver_maintenance_schedules` | `silver/dim_maintenance` |
| `silver_dim_date` | `dim_date` | `silver/dim_date` |

### Daily Tasks

Runs every night after the yearly gate clears. Ingests vehicle registry and assignment
data, captures a safety snapshot, then builds the Silver and Gold fleet tables.

| Task | Glue Job | Output |
|---|---|---|
| `daily_bronze_midnight` | `bronze_ingest` | Bronze ingested path (datasets: `vehicle_registry`, `vehicle_assignment`) |
| `daily_safety_snapshot` | `daily_safety_snapshot` | `gold/daily_safety_snapshot` |
| `silver_vehicle_registry` | `silver_vehicle_registry` | `silver/vehicle_registry` |
| `silver_vehicle_assignment` | `silver_vehicle_assignment` | `silver/vehicle_assignment` |
| `gold_active_fleet_snapshot` | `gold_active_fleet_snapshot` | `gold/active_fleet_snapshot` |

`silver_vehicle_assignment` reads from Silver vehicle registry, Silver maintenance
schedules (written by the yearly tasks), and PostgreSQL — so the yearly gate must pass
before daily work begins.

### Monthly Tasks

Runs once per calendar month. Controlled by the Airflow Variable
`omniroute_monthly_done_YYYY_MM` (e.g. `omniroute_monthly_done_2026_05`).

Same retry logic as yearly: if the monthly tasks fail on the 1st, they retry on the
2nd, 3rd, etc. until success, at which point `mark_monthly_done` sets the variable to
`"true"`.

| Task | Glue Job | Output |
|---|---|---|
| `monthly_rate_deduction_report` | `monthly_rate_deduction_report` | `gold/monthly_rate_deduction` |
| `safety_strikes_reset` | `safety_strikes_reset` | Writes to PostgreSQL |

### Trigger Rules

Two non-default trigger rules are used to handle the branching correctly:

- `yearly_gate`, `monthly_gate`, and `end` all use `none_failed_min_one_success` —
  this allows either the "run" branch or the "skip" branch to satisfy the gate,
  as long as neither has failed.

---

## 2. Morning Fuel Pipeline (`omniroute_morning_fuel_pipeline`)

**Schedule:** Daily at `05:00 UTC`  
**Trigger:** Automatic (cron)  
**Max Active Runs:** 1

Runs 5 hours after the midnight pipeline. Ingests fuel transactions, builds the Silver
fuel table, computes the Gold fuel efficiency audit, and writes reporting tables back to
PostgreSQL.

### Dependency Graph

```
daily_bronze_fuel
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
```

### Tasks

| Task | Glue Job | Output |
|---|---|---|
| `daily_bronze_fuel` | `bronze_ingest` | Bronze ingested path (dataset: `fuel_transactions`) |
| `silver_fuel_transactions` | `silver_fuel_transactions` | `silver/fuel_transactions` |
| `gold_fuel_efficiency_audit` | `gold_fuel_efficiency_audit` | `gold/fuel_efficiency_audit` |
| `gold_to_postgres` | `gold_to_postgres` | PostgreSQL reporting tables |

`silver_fuel_transactions` joins against `silver/vehicle_registry`,
`silver/vehicle_assignment`, and `silver/dim_maintenance` — all written by the midnight
pipeline — so this DAG must not run before midnight completes.

<!-- ### ⚠️ Known Issue — ExternalTaskSensor Disabled

The DAG contains a commented-out `ExternalTaskSensor` that was intended to gate this
pipeline on the successful completion of `omniroute_midnight_pipeline`:

```python
# wait_for_midnight = ExternalTaskSensor(
#     external_dag_id="omniroute_midnight_pipeline",
#     external_task_id="end",
#     execution_delta=timedelta(hours=5),
#     ...
# )
```

The `execution_delta=timedelta(hours=5)` maps the morning DAG's `05:00` logical date
back to the midnight DAG's `00:00` run for the same calendar day. Until this is
re-enabled, the pipeline relies on the 5-hour gap being sufficient. If the midnight
pipeline is delayed or reruns, the morning pipeline may read stale or incomplete Silver
tables. -->

<!-- **To re-enable:** uncomment the sensor block and restore `wait_for_midnight >>
daily_bronze_fuel` as the first dependency. -->

---

## 3. EMR Streaming Pipeline (`omniroute_emr_streaming_pipeline`)

**Schedule:** Manual trigger only (`schedule=None`)  
**Trigger:** Manual  
**Max Active Runs:** 1

Provisions a dedicated EMR cluster and runs three concurrent Spark Structured Streaming
jobs that continuously move data from Kafka through the Bronze → Silver → Gold medallion
layers using Delta Lake.

### Dependency Graph

```
start
  │
  ▼
create_emr_cluster
  │
  ▼
add_streaming_steps
  │
  ├──► watch_bronze_step ──┐
  ├──► watch_silver_step ──┤
  └──► watch_gold_step  ───┘
                           │
                           ▼
               terminate_emr_cluster
                           │
                           ▼
                          end
```

### EMR Cluster Configuration

| Property | Value |
|---|---|
| Release | `emr-7.12.0` |
| Master | 1× `m5a.xlarge` (ON_DEMAND) |
| Core | 3× `m5a.xlarge` (ON_DEMAND) |
| Task | 1× `m5a.xlarge` (ON_DEMAND) |
| EBS (per node) | 2× 32 GB gp2 |
| Root volume | 40 GB |
| Step concurrency | 3 (all steps run in parallel) |
| Applications | Hadoop, Hive, Spark, Livy, JupyterEnterpriseGateway |
| Bootstrap script | `emr/bootstrap_omniroute.sh` |

### Streaming Steps

All three steps are submitted in a single `EmrAddStepsOperator` call and run
concurrently thanks to `StepConcurrencyLevel: 3`. Each is monitored by a dedicated
`EmrStepSensor`.

| Step | Script | Kafka Package | Notes |
|---|---|---|---|
| `OmniRoute_Bronze_Streaming` | `bronze_streaming.py` | `spark-sql-kafka-0-10_2.12:3.5.6` | Reads from Kafka, writes Delta to S3 Bronze |
| `OmniRoute_Silver_Streaming` | `silver_streaming.py` | — | Reads Bronze Delta, writes Silver Delta |
| `OmniRoute_Gold_Streaming` | `gold_streaming.py` | — | Reads Silver Delta, writes Gold Delta |

All steps use `--deploy-mode client`, 2 GB executor memory, 1 GB driver memory, and
`spark.yarn.maxAppAttempts=1` (no automatic Spark-level retries).

`KAFKA_SERVER` (`172.31.65.131:9092`) is injected into both the executor and driver
environments via `spark.executorEnv` and `spark.driverEnv` conf flags.

### Cluster Lifecycle

`terminate_emr_cluster` uses `trigger_rule="all_done"` — the cluster is terminated
whether the streaming steps succeed or fail. Each step individually uses
`ActionOnFailure: CONTINUE`, so one failing stream does not kill the other two before
termination.

---

## Shared Configuration

### `s3_paths.json`

All three DAGs load S3 paths and Glue job configurations from the JSON Airflow Variable
`omniroute_config` (the contents of `config/s3_paths.json`), read at the top of each DAG
file. If the Variable is missing or not valid JSON, the DAGs
fail at parse time with an import error naming the Variable, before any task runs.
See [batch_architecture.md](batch_architecture.md#configuration-reference) for the schema.

### Glue Job Convention

Every Glue task is built by the DAG's `build_glue_task(task_id, job, script_args)` helper,
which reads `glue.jobs.<job>` merged over `glue.defaults`. The default `DefaultArguments`
enable Delta Lake:

```python
"--datalake-formats": "delta",
"--job-bookmark-option": "job-bookmark-disable",
```

Bronze ingestion jobs write plain Parquet, so they override this to bookmark-disable only.
Job bookmarks are intentionally disabled — idempotency is handled at the script level,
not by Glue's bookmark mechanism.

### PostgreSQL Connection

Tasks that write to or read from PostgreSQL receive connection details via Airflow
Variables: `pg_host`, `pg_port` (default `5432`), `pg_database`, `pg_user`,
`pg_password`. These must be set before the DAGs are enabled. See `operations.md` for
the full variable reference.
