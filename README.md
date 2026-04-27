# 🚛 OmniRoute — Smart Logistics Engine

## Overview

OmniRoute is a production-grade data engineering pipeline that processes batch and real-time logistics data to monitor fleet operations, driver safety, and fuel efficiency using a **Modern Data Lakehouse** architecture.

It combines:
- **Batch data** from AWS S3 (vehicle registry, assignments, fuel logs, maintenance schedules)
- **Real-time telemetry** from Apache Kafka (GPS coordinates, speed, sensor data)

The system generates actionable insights including:
- Driver safety violations with penalty enforcement
- Fuel efficiency anomalies (>12% deviation from model baseline)
- Vehicle assignment history using SCD Type 2
- Active fleet snapshots and compliance reporting

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Orchestration** | Apache Airflow | DAG scheduling (daily/monthly/yearly) |
| **Batch Processing** | PySpark (EMR Serverless) | Bronze ingestion, Silver transforms, Gold aggregations |
| **Stream Processing** | Spark Structured Streaming | Kafka telemetry → violation detection |
| **Message Broker** | Apache Kafka (MSK) | Real-time vehicle telemetry transport |
| **Storage** | AWS S3 (Parquet) | Medallion architecture: Bronze → Silver → Gold |
| **Reporting DB** | PostgreSQL (RDS) | Report-ready tables for BI tools |
| **Language** | Python 3.10+ | All scripts, producers, and pipeline logic |

---

## Project Structure

```
omniroute/
├── dags/                                  # Airflow DAG definitions
│   ├── omniroute_daily_batch.py           #   Daily: Bronze → DQ Gate → Silver → Gold → Reporting
│   ├── omniroute_monthly_cooldown.py      #   Monthly: Reset driver strikes → rate deduction report
│   └── omniroute_yearly_maintenance.py    #   Yearly: Ingest maintenance schedules
│
├── spark_jobs/                            # PySpark batch jobs
│   └── batch/
│       ├── ingest_vehicle_registry.py     #   Bronze: Full snapshot CSV → Parquet
│       ├── ingest_vehicle_assignment.py   #   Bronze: Incremental CSV → Parquet
│       ├── ingest_fuel_transactions.py    #   Bronze: Daily fuel CSV → Parquet
│       └── ingest_maintenance_schedules.py#   Bronze: Yearly maintenance CSV → Parquet
│
├── data/
│   ├── raw/                               # Source CSV/JSON files
│   ├── processed/                         # Streaming output (violations, safety status)
│   └── scripts/                           # Data generators & Kafka consumer
│       ├── config.py                      #   Centralized configuration
│       ├── run_all_producers.py           #   Generate all batch data
│       ├── vehicle_registry_producer.py
│       ├── vehicle_assignment_producer.py
│       ├── fuel_transactions_producer.py
│       ├── maintenance_schedules_producer.py
│       ├── restricted_zones_producer.py
│       ├── telemetry_producer.py          #   Kafka real-time producer
│       └── consumer_scripts/
│           └── spark_streaming_consumer.py
│
├── docs/                                  # Architecture & design documentation
│   ├── omniroute_architecture_deep_dive.md
│   ├── DAG.md
│   ├── data.md
│   ├── data_edge_case_explain.md
│   ├── schema_evolution.md
│   ├── delta_lakehouse_deep_dive.md
│   ├── postgres_setup_guide.md
│   └── ec2_run_guide.txt
│
├── .env.example                           # Environment variable template
├── Makefile                               # Operations automation
├── requirements.txt                       # Python dependencies
└── README.md                              # This file
```

---

## Objectives

- Track vehicle-driver history using **SCD Type 2**
- Detect abnormal fuel consumption (**>12% deviation** from model baseline)
- Monitor real-time safety violations (**speeding >110 km/h**, restricted zone breaches)
- Apply **penalty system** based on safety strikes (5% deduction per strike, suspension at 10)
- Generate **daily and monthly reports** (CSV/TXT exports to S3)
- Enforce **monthly cooldown** (reset strikes for non-suspended drivers)

---

## DAG Schedules

| DAG | Schedule | Layers | Description |
|---|---|---|---|
| `omniroute_daily_batch` | `0 5 * * *` | Bronze → DQ Gate → Silver → Gold → Reporting | Core pipeline with S3 sensors and quality gates |
| `omniroute_monthly_cooldown` | `0 5 1 * *` | Gold → Reporting | Reset driver strikes, generate rate deduction report |
| `omniroute_yearly_maintenance` | `0 0 1 1 *` | Bronze → Silver | Ingest and clean maintenance schedules |
| Streaming (always-on) | Continuous | Bronze → Silver → Gold | Kafka telemetry → violations → driver safety |

---

## Quick Start

### 1. Clone and setup

```bash
git clone https://github.com/Saint-Potato/omniroute
cd omniroute
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your AWS credentials, Kafka server, PostgreSQL details
export $(grep -v '^#' .env | xargs)
```

### 3. Generate test data

```bash
cd data/scripts && python run_all_producers.py
```

### 4. Deploy Spark jobs and DAGs

```bash
sudo mkdir -p /opt/omniroute/spark_jobs
sudo cp -r spark_jobs/* /opt/omniroute/spark_jobs/
cp dags/*.py ~/airflow/dags/
```

### 5. Start Airflow

```bash
airflow db init                    # First-time only
airflow scheduler &
airflow webserver --host 0.0.0.0 --port 8080
```

### 6. Run batch pipeline manually (optional)

```bash
spark-submit spark_jobs/batch/ingest_vehicle_registry.py --run-date 2026-04-16
spark-submit spark_jobs/batch/ingest_vehicle_assignment.py --run-date 2026-04-16
spark-submit spark_jobs/batch/ingest_fuel_transactions.py --run-date 2026-04-16
```

### 7. Start streaming consumer

```bash
spark-submit data/scripts/consumer_scripts/spark_streaming_consumer.py
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `LANDING_PATH` | `s3a://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/landing` | S3 path for raw CSV uploads |
| `INGESTED_PATH` | `s3a://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/ingested/` | S3 path for validated Parquet |
| `QUARANTINE_PATH` | `s3a://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/quarantine` | S3 path for rejected files |
| `METRICS_PATH` | `s3a://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/metrics` | S3 path for DQ metrics JSON |
| `KAFKA_SERVER` | `localhost:9092` | Kafka bootstrap server |
| `PG_HOST` | `localhost` | PostgreSQL host |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_DB` | `omniroute_dwh` | PostgreSQL database |

---

## Data Sources

| Source | Format | Frequency | Key Fields |
|---|---|---|---|
| Vehicle Registry | CSV | Daily (full snapshot) | `vin`, `model`, `mfg_year`, `fuel_type` |
| Vehicle Assignment | CSV | Daily (incremental) | `vin`, `driver_id`, `start_timestamp`, `end_timestamp`, `daily_rate`, `region` |
| Maintenance Schedules | CSV | Yearly (Jan 1st) | `vin`, `service_date`, `service_type` |
| Fuel Transactions | CSV | Daily | `transaction_id`, `vin`, `fuel_liters`, `odometer_reading`, `timestamp` |
| Telemetry Stream | JSON/Kafka | Real-time | `vin`, `driver_id`, `speed`, `lat`, `long`, `event_timestamp` |
| Restricted Zones | JSON | Static | `zone_name`, `min_lat`, `max_lat`, `min_long`, `max_long` |

---

## Production Features

- **S3 Key Sensors** — DAG waits for source files before ingestion
- **DQ Quality Gate** — Pipeline blocks if data pass rate drops below 95%
- **Data Quality Metrics** — JSON metrics emitted per job per run for observability
- **Dynamic Partition Overwrite** — Idempotent writes (safe re-runs)
- **Quarantine** — Invalid rows isolated with UUID + batch_id for forensics
- **SLA Monitoring** — Pipeline must complete within 2 hours
- **Failure Callbacks** — Alerting integration for task failures
- **Schema Pre-Validation** — Two-pass validation (header check + type enforcement)
- **Structured Logging** — Named loggers with log levels (not print statements)
- **Spark Tuning** — AQE, snappy compression, fast file commits