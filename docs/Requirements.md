# OmniRoute Smart Logistics Engine — Requirements

## 1. Introduction

The OmniRoute system is an enterprise-grade Data Warehouse (DWH) solution designed to ingest, process, and analyze high-velocity telemetry data from a global fleet of delivery vehicles. It integrates **batch data from AWS S3** (vehicle registries, assignment logs, and maintenance schedules) with **real-time IoT streams from Kafka** (GPS and sensor data) to monitor driver safety and asset health.

---

## 2. Business Objectives

| # | Objective | Description |
|---|-----------|-------------|
| 1 | **Asset Lifecycle Management** | Maintain an accurate, historical record of vehicle assignments and driver transitions using SCD Type 2 logic. |
| 2 | **Operational Efficiency** | Detect vehicles with abnormal fuel consumption patterns compared to fleet baselines. |
| 3 | **Safety Compliance** | Real-time monitoring of driver behavior (speeding and geofencing) to mitigate risk. |
| 4 | **Financial Accountability** | Automate driver rate deductions based on safety violations and manage monthly cooldown periods. |

---

## 3. System Requirements

### 3.1 Data Sources & Frequency

| Data Asset | Format | Frequency | Description |
|---|---|---|---|
| Vehicle Registry | CSV | Daily (00:00 UTC) | Master list of all fleet vehicles (`vin`, `model`, `mfg_year`). Full snapshot. |
| Vehicle Assignment | CSV | Daily (00:00 UTC, Incremental) | History of driver-to-vehicle assignments including `daily_rate`. |
| Maintenance Logs | CSV | Yearly (Jan 1st) | Scheduled downtime and mandatory service dates for the fleet. |
| Fuel Transactions | CSV | Daily (07:00 UTC) | Logs of fuel quantity, cost, and odometer readings. |
| Telemetry Stream | JSON | Real-time (Kafka) | Live sensor data: `vin`, `speed`, `rpm`, and `location_coords`. |
| Restricted Zones | JSON | Static / Ad-hoc | GPS coordinates defining no-go zones. |

### 3.2 Detailed Data Schemas

#### 3.2.1 Vehicle Registry (`vehicle_registry.csv`)

Master dimension table, delivered as a full snapshot daily.

| Column | Type |
|---|---|
| `vin` | String |
| `model` | String |
| `mfg_year` | Integer |
| `fuel_type` | String |

**Sample:**

| vin | model | mfg_year | fuel_type |
|---|---|---|---|
| 1HGBH225 | Freightliner M2 | 2022 | Diesel |
| 3FA6P0HD | Volvo VNL | 2023 | LNG |

#### 3.2.2 Vehicle Assignment (`vehicle_assignment.csv`)

Tracks driver transitions and pay rates via daily incremental updates.

| Column | Type |
|---|---|
| `vin` | String |
| `driver_id` | String |
| `start_timestamp` | Unix Timestamp |
| `end_timestamp` | Unix Timestamp / Null |
| `daily_rate` | Float |
| `region` | String |

> [!IMPORTANT]
> Convert Unix timestamps to date format and ensure record continuity during ingestion.

**Sample:**

| vin | driver_id | start_timestamp | end_timestamp | daily_rate |
|---|---|---|---|---|
| 1HGBH225 | DRV_902 | 1712818800 | 1713078000 | 450.00 |
| 1HGBH225 | DRV_902 | 1713078000 | — | 485.00 |

#### 3.2.3 Maintenance Logs (`maintenance_schedules.csv`)

Mandatory downtime records, over and above standard working days.

| Column | Type |
|---|---|
| `vin` | String |
| `service_date` | Date |
| `service_type` | String |

**Sample:**

| vin | service_date | service_type |
|---|---|---|
| 1HGBH225 | 2026-06-15 | Engine Overhaul |
| 3FA6P0HD | 2026-08-20 | Tire Rotation |

#### 3.2.4 Fuel Transactions (`fuel_transactions.csv`)

Fuel usage logs for the 12% efficiency threshold calculation.

| Column | Type |
|---|---|
| `transaction_id` | String |
| `vin` | String |
| `fuel_liters` | Float |
| `odometer_reading` | Float |
| `timestamp` | UTC Timestamp |

**Sample:**

| transaction_id | vin | fuel_liters | odometer_reading | timestamp |
|---|---|---|---|---|
| TXN_550 | 1HGBH225 | 120.5 | 45200.0 | 2026-04-14 06:45:00 |

#### 3.2.5 Telemetry Stream (Kafka)

Real-time JSON objects for safety flagging.

| Column | Type |
|---|---|
| `vin` | String |
| `driver_id` | String |
| `speed` | Integer |
| `lat` | Float |
| `long` | Float |
| `event_timestamp` | Kafka Timestamp |

**Sample:**
```json
{
  "vin": "1HGBH225",
  "driver_id": "DRV_902",
  "speed": 115,
  "lat": 28.6139,
  "long": 77.2090
}
```

#### 3.2.6 Restricted Zones (`restricted_zones.json`)

Reference file for Safety Strike zone detection.

| Column | Type |
|---|---|
| `zone_name` | String |
| `min_lat` | Float |
| `max_lat` | Float |
| `min_long` | Float |
| `max_long` | Float |

**Sample:**
```json
[
  {
    "zone_name": "High_Risk_Pass_A",
    "min_lat": 34.05,
    "max_lat": 34.10,
    "min_long": -118.25,
    "max_long": -118.20
  }
]
```

---

### 3.3 Advanced Data Processing Logic

#### 3.3.1 Asset History — SCD Type 2

- Maintain an incremental table tracking `vin`, `start_date`, `end_date`, `driver_id`, `daily_rate`, and `status`.
- **Conflict Resolution:** If duplicate records exist for the same VIN and timeframe, retain only the record with the **highest `daily_rate`** (use `ROW_NUMBER()` partitioned by `vin` and `start_date` ordered by `daily_rate DESC`).
- **Continuity:** When a new assignment record is ingested, close the previous record (`end_date` = new `start_date`) and mark it as `ARCHIVED`.
- **Active Status:** Records without an `end_date` are marked as `IN-TRANSIT`.

**Example — Driver Swap:**

| Step | vin | driver_id | start_date | end_date | daily_rate | status |
|---|---|---|---|---|---|---|
| Before | VIN-100 | DRV-001 | 2026-04-01 | NULL | 500 | IN-TRANSIT |
| After (Row 1) | VIN-100 | DRV-001 | 2026-04-01 | 2026-04-15 | 500 | ARCHIVED |
| After (Row 2) | VIN-100 | DRV-002 | 2026-04-15 | NULL | 550 | IN-TRANSIT |

#### 3.3.2 Fuel Efficiency Auditing

- Flag vehicles where *Fuel Consumed vs. Distance* exceeds the fleet baseline by **12%**.
- **Exclude** weekends and maintenance days from the calculation.
- Generate a daily report by **05:00 UTC** showing the count of `IN-TRANSIT` vehicles categorized by model.

**Example — Outlier Detection:**

| Metric | Value |
|---|---|
| Fleet baseline (Freightliner M2) | 5.0 km/L |
| 12% threshold | 4.4 km/L |
| Vehicle VIN-300 consumption | 100 L over 400 km = **4.0 km/L** |
| **Result** | **FLAGGED** (4.0 < 4.4) |

> [!CAUTION]
> If a day falls on a weekend or the vehicle is listed in `maintenance_schedules.csv` for that day, that day's fuel data **must be excluded** to avoid penalizing drivers for workshop idling.

---

### 3.4 Safety & Streaming Requirements

#### 3.4.1 Violation Detection

A streaming pipeline must flag events where:
- Speed exceeds **110 km/h**, OR
- Coordinates intersect with `restricted_zones.json`.

> [!NOTE]
> A single event window triggering both speed and zone violations should be treated as **one Safety Strike** to prevent over-penalization.

#### 3.4.2 Penalty System

- Each flagged event counts as one **Safety Strike**.
- Deduct **5%** from the driver's `daily_rate` per strike.
- The deduction is represented as a separate calculated column (`current_adjusted_rate`) — the original `base_rate` must never be modified.

**Example — Strike Calculation:**

| driver_id | base_rate | strike_count | current_adjusted_rate |
|---|---|---|---|
| DRV-A | 500 | 1 | 475 |

#### 3.4.3 Cooldown & Suspension

- **Monthly Cooldown:** On the 1st of every month at **05:00 UTC**, reset strikes and restore rates for eligible drivers.
- **Suspension:** Drivers who accumulate **10 strikes** are toggled to `SUSPENDED` status and are **excluded** from the monthly cooldown.

| Scenario | Trigger | Result |
|---|---|---|
| Standard Cooldown | Month rolls over, strikes < 10 | Strikes → 0, rate restored to `base_rate` |
| Suspension | 10th strike hit | Status → `SUSPENDED`, excluded from cooldown, rate remains penalized |

---

## 4. Technical Architecture

The solution must be built exclusively on **AWS** following DWH principles for fault tolerance and scalability.

| Component | Technology |
|---|---|
| **Storage** | S3 (Data Lake) — raw, silver, and gold layers |
| **Batch Compute** | Spark / Glue for batch ETL |
| **Stream Compute** | Flink or Spark Streaming for Kafka telemetry |
| **Database** | PostgreSQL on EC2 (or RDS) for final reporting |
| **Orchestration** | Airflow — manages dependencies and daily scheduling at 05:00 UTC |

---

## 5. Reporting & Analytics Layer

### 5.1 Reporting Objectives

- Provide timely, consistent, and auditable views of fleet operations.
- Enable daily and monthly management reporting without querying raw or streaming systems.
- Serve as a **single source of truth** for finance, operations, and compliance teams.
- Decouple analytical workloads from ETL and streaming pipelines.

### 5.2 Reporting Architecture

```
Gold Layer (S3 – Parquet)
│
│  (Scheduled Loads / Incrementals)
▼
Reporting Database (RDS/PostgreSQL or Redshift)
│
├── BI Dashboards
├── Scheduled Reports (CSV / TXT)
└── Ad-hoc SQL Access (Read-only)
```

| Component | Technology |
|---|---|
| Reporting DB | PostgreSQL on EC2 / Amazon RDS (or Redshift) |
| Data Transport | Spark / Glue |
| Query Access | SQL |
| BI Tools | QuickSight / Power BI / Tableau (optional) |

### 5.3 Reporting Data Models

#### 5.3.1 Fleet Assignment History Report

- **Source:** `gold.asset_history_scd2`
- **Purpose:** Historical traceability of drivers, vehicles, and rate changes.
- **Key Columns:** `vin`, `driver_id`, `start_date`, `end_date`, `daily_rate`, `status`, `region`
- **Usage:** Driver audits, legal & compliance investigations, historical payroll validation.

#### 5.3.2 Active Fleet Snapshot Report

- **Source:** `gold.active_fleet_snapshot`
- **Refresh:** Daily @ 05:00 UTC
- **Purpose:** Executive-level fleet utilization monitoring.

| Column | Description |
|---|---|
| `model` | Vehicle model |
| `no_of_active_vehicles` | Count of `IN-TRANSIT` vehicles |
| `snapshot_time` | Snapshot timestamp |

#### 5.3.3 Fuel Efficiency Audit Report

- **Source:** `gold.fuel_efficiency_audit`
- **Purpose:** Identify abnormal fuel consumption and potential misuse.
- **Key Fields:** `vin`, `model`, `audit_date`, `km_per_liter`, `baseline_kmpl`, `status` (`FLAGGED` / `OK`)
- **Business Use:** Cost control, driver performance review, maintenance planning.

#### 5.3.4 Driver Safety & Penalty Report

- **Source:** `gold.driver_safety_status`
- **Purpose:** Near-real-time and monthly payroll impact analysis.

| Field | Description |
|---|---|
| `driver_id` | Unique driver identifier |
| `base_rate` | Original daily rate |
| `strike_count` | Active month strike count |
| `current_adjusted_rate` | Penalized daily rate |
| `status` | `ACTIVE` / `SUSPENDED` |
| `month` | Reporting month |

### 5.4 Scheduled & Regulatory Reports

#### 5.4.1 Monthly Driver Rate Deduction Report

Includes: Driver ID, total strikes for the month, total rate deductions, final payable daily rate, and suspension status (if applicable).

#### 5.4.2 Safety Compliance Summary

- **Purpose:** Monitor safety trends and high-risk behavior.
- **Metrics:** Total violations by day, top 10 drivers by strike count, restricted zone breaches, speed violation counts.

---

## 6. Success Criteria

| # | Criterion |
|---|---|
| 1 | Successful ingestion and deduplication of vehicle assignment logs from S3. |
| 2 | Automated generation of monthly rate-deduction reports in text format for fleet managers. |
| 3 | Zero-duplicate processing in the event of job failure (**Idempotency**). |
| 4 | Functional real-time system tracking active safety strikes across the fleet. |

---

## 7. Technical Implementation Guidance

### Batch Processing
- Use **Spark/Glue** for SCD Type 2 logic and deduplication.
- Ensure the **Fuel Audit runs after** the Asset History update to use the most recent `daily_rate` and `status`.
- **Fault Tolerance:** Job restarts must not create duplicate `ARCHIVED` records for the same day.

### Streaming Processing
- **State Management:** Use a stateful streaming approach to maintain an active strike count in memory or a fast-access DB (Redis / DynamoDB) before flushing to the DWH.
- **Idempotency:** Monthly cooldown job must not reset the same driver twice or accidentally reset a `SUSPENDED` driver.
- **Join Strategy:** Join the Kafka `vin` with the Asset History table to resolve the `driver_id` currently assigned to the vehicle.
