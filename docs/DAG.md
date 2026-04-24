# OmniRoute — Airflow DAG Documentation

DAGs are organized by **schedule frequency**, with **TaskGroups** inside each DAG to represent layer boundaries (Bronze → Silver → Gold → Reporting).

## DAG Overview

| DAG | Schedule (cron) | Layers | Description |
|---|---|---|---|
| `omniroute_daily_batch` | `0 5 * * *` (daily 05:00 UTC) | Bronze → Silver → Gold → Reporting | Core pipeline: ingest registry, assignment, fuel → transform → SCD2, fuel audit, fleet snapshot → load Postgres → generate reports |
| `omniroute_monthly_cooldown` | `0 5 1 * *` (1st of month 05:00 UTC) | Gold → Reporting | Reset strikes for eligible drivers, generate rate deduction report |
| `omniroute_yearly_maintenance` | `0 0 1 1 *` (Jan 1st 00:00 UTC) | Bronze → Silver | Ingest maintenance_schedules.csv, clean and deduplicate |
| `omniroute_streaming` | Externally managed (always-on) | Bronze → Silver → Gold | Kafka telemetry → validate → detect violations → update driver safety |

---

## 1. `omniroute_daily_batch`

```
Schedule: 0 5 * * * (daily @ 05:00 UTC)
Catchup:  False
Tags:     [batch, daily, core]
SLA:      2 hours (pipeline must complete within this window)
```

### Production Features

| Feature | Implementation | Purpose |
|---|---|---|
| **S3KeySensor** | Waits for each CSV in `landing/` before ingestion | Prevents ingestion of missing files |
| **DQ Quality Gate** | Reads DQ metrics JSON, blocks if pass rate < 95% | Prevents bad data from reaching Silver |
| **SLA Monitoring** | `sla=timedelta(hours=2)` in `default_args` | Alerts if pipeline runs too long |
| **Failure Callbacks** | `on_failure_callback` logs dag_id, task_id, log_url | Integration point for Slack/SNS/PagerDuty |

### Task Dependency Chain

```mermaid
flowchart LR
    subgraph sensors["S3 Sensors"]
        S1["wait_for_registry"]
        S2["wait_for_assignment"]
        S3["wait_for_fuel"]
    end

    subgraph bronze["TaskGroup: bronze"]
        BI1[ingest_registry]
        BI2[ingest_assignment]
        BI3[ingest_fuel]
    end

    DQ["dq_quality_gate"]

    subgraph silver["TaskGroup: silver"]
        TS1[transform_assignment]
        TS2[transform_fuel]
    end

    subgraph gold["TaskGroup: gold"]
        G1[build_scd2]
        G2[build_fuel_audit]
        G3[build_fleet_snapshot]
    end

    subgraph reporting["TaskGroup: reporting"]
        R1[load_postgres]
        R2[generate_reports]
    end

    S1 --> BI1
    S2 --> BI2
    S3 --> BI3

    BI1 --> DQ
    BI2 --> DQ
    BI3 --> DQ

    DQ --> TS1
    DQ --> TS2
    BI1 --> TS1
    BI1 --> TS2

    TS1 --> G1
    G1 --> G2
    TS2 --> G2
    G1 --> G3

    G2 --> R1
    G3 --> R1
    R1 --> R2
```

### Task Details

| TaskGroup | Task | Type / Spark Job | Description |
|---|---|---|---|
| sensors | `wait_for_vehicle_registry` | S3KeySensor | Waits for `landing/vehicle_registry.csv` (1hr timeout, reschedule mode) |
| sensors | `wait_for_vehicle_assignment` | S3KeySensor | Waits for `landing/vehicle_assignment.csv` (1hr timeout, reschedule mode) |
| sensors | `wait_for_fuel_transactions` | S3KeySensor | Waits for `landing/fuel_transactions.csv` (1hr timeout, reschedule mode) |
| bronze | `ingest_vehicle_registry` | `batch/ingest_vehicle_registry.py` | Full snapshot CSV → Parquet (overwrite) |
| bronze | `ingest_vehicle_assignment` | `batch/ingest_vehicle_assignment.py` | Incremental CSV → Parquet (append) |
| bronze | `ingest_fuel_transactions` | `batch/ingest_fuel_transactions.py` | Daily fuel CSV → Parquet (append) |
| — | `dq_quality_gate` | PythonOperator | Reads DQ metrics JSON from S3, fails pipeline if any job pass rate < 95% |
| silver | `transform_vehicle_assignment` | `batch/transform_vehicle_assignment.py` | Unix→date, dedup by highest rate |
| silver | `transform_fuel_transactions` | `batch/transform_fuel_transactions.py` | Weekend/maintenance filter, compute km/liter |
| gold | `build_asset_history_scd2` | `batch/build_asset_history_scd2.py` | SCD Type 2 merge |
| gold | `build_fuel_efficiency_audit` | `batch/build_fuel_efficiency_audit.py` | 12% threshold audit |
| gold | `build_active_fleet_snapshot` | `batch/build_active_fleet_snapshot.py` | IN-TRANSIT count by model |
| reporting | `load_reporting_db` | `batch/load_reporting_db.py` | Gold → PostgreSQL via JDBC |
| reporting | `generate_reports` | `batch/generate_reports.py` | CSV/TXT exports to S3 |

---

## 2. `omniroute_monthly_cooldown`

```
Schedule: 0 5 1 * * (1st of month @ 05:00 UTC)
Catchup:  False
Tags:     [batch, monthly, safety]
```

### Tasks

```mermaid
flowchart LR
    subgraph gold["TaskGroup: gold"]
        MC1[reset_driver_strikes]
    end
    subgraph reporting["TaskGroup: reporting"]
        MC2[generate_rate_deduction_report]
    end
    MC1 --> MC2
```

### Logic

- Reset `strike_count = 0` and `current_adjusted_rate = base_rate` for drivers `WHERE status != 'SUSPENDED'`
- Generate monthly rate deduction TXT report to S3
- Idempotent: uses `last_reset_month` to prevent double resets on retry

---

## 3. `omniroute_yearly_maintenance`

```
Schedule: 0 0 1 1 * (Jan 1st @ 00:00 UTC)
Catchup:  False
Tags:     [batch, yearly, maintenance]
```

### Tasks

```mermaid
flowchart LR
    subgraph bronze["TaskGroup: bronze"]
        YB1[ingest_maintenance_logs]
    end
    subgraph silver["TaskGroup: silver"]
        YS1[clean_maintenance_logs]
    end
    YB1 --> YS1
```

### Logic

- Ingest `maintenance_schedules.csv` from `landing/` → validate → write to `ingested/maintenance_schedules/`
- Deduplicate by `(vin, service_date)` → write to Silver
- No Gold/Reporting step — maintenance data is a lookup table consumed by the daily fuel audit

---

## 4. `omniroute_streaming`

```
Schedule: Externally managed (always-on Spark Streaming job)
Tags:     [streaming, safety, real-time]
```

### Pipeline

```mermaid
flowchart LR
    subgraph bronze["Bronze"]
        ST1["Kafka → ingested/telemetry_raw"]
    end
    subgraph silver["Silver"]
        ST2["Validate & flag violations"]
    end
    subgraph gold["Gold"]
        ST3["Write safety_violations"]
        ST4["Update driver_safety_status"]
    end
    ST1 --> ST2 --> ST3 --> ST4
```

### Logic

- Spark Structured Streaming reads Avro from Kafka
- Validates events, flags speeding (`> 110 km/h`) and restricted zone breaches
- Joins with `gold.asset_history_scd2` to resolve `driver_id` from `vin`
- Appends violations and updates driver safety status in near-real-time

---

## Configuration

| Parameter | Value | Source |
|---|---|---|
| `SPARK_SUBMIT` | `spark-submit` | DAG file |
| `JOBS_DIR` | `/opt/omniroute/spark_jobs` | DAG file |
| `RUN_DATE` | `{{ ds }}` (Airflow execution date) | Airflow template |
| Retries | 2 | `default_args` |
| Retry delay | 5 minutes | `default_args` |
| Max active runs | 1 | DAG config |
