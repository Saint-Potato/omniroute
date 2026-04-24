# Silver Layer Spark Transformation Scripts

## Goal
Create 4 Silver layer PySpark transformation scripts that read Bronze Parquet data and write cleansed, enriched data as **Delta Lake** tables to the Silver layer. Also update the Airflow DAGs to orchestrate these new tasks.

## Proposed Changes

### Silver Transformation Scripts

Based on the BRD (Section 3.3) and `medallion_architecture.md`, we need **4 Silver tables**:

| # | Script | Bronze Source | Silver Target | Key Transformations |
|---|--------|-------------|---------------|---------------------|
| 1 | `transform_vehicle_registry.py` | `ingested/vehicle_registry` (Parquet) | `silver/vehicle_registry_clean` (Delta) | Dedup by VIN, drop NULL VINs, TRIM/UPPER model & fuel_type, validate fuel types, drop Bronze metadata |
| 2 | `transform_vehicle_assignment.py` | `ingested/vehicle_assignment` (Parquet) | `silver/vehicle_assignment_clean` (Delta) | Unix→Date conversion, dedup by (vin, start_date) keeping highest daily_rate via `ROW_NUMBER()`, drop NULL vin/driver_id, TRIM/UPPER region, drop Bronze metadata |
| 3 | `transform_maintenance_schedules.py` | `ingested/maintenance_schedules` (Parquet) | `silver/maintenance_schedules` (Delta) | Parse service_date to Date type, dedup by (vin, service_date), drop invalid dates, drop invalid VINs, TRIM service_type |
| 4 | `transform_fuel_transactions.py` | `ingested/fuel_transactions` (Parquet) | `silver/fuel_transactions_enriched` (Delta) | Dedup by transaction_id, parse timestamp, compute `txn_date`, `day_of_week`, `is_weekend`, JOIN with maintenance to flag `is_maintenance_day`, `LAG()` for `prev_odometer` & `distance_km`, compute `km_per_liter`, filter out invalid fuel/odometer |

---

### File Structure

#### [NEW] `spark_jobs/batch/transform_vehicle_registry.py`
- Read latest Bronze partition Parquet
- Dedup, clean, validate → write Delta to Silver

#### [NEW] `spark_jobs/batch/transform_vehicle_assignment.py`
- Read Bronze Parquet
- Unix timestamp → Date, ROW_NUMBER dedup, clean → write Delta to Silver

#### [NEW] `spark_jobs/batch/transform_maintenance_schedules.py`
- Read Bronze Parquet
- Date parsing, dedup, clean → write Delta to Silver

#### [NEW] `spark_jobs/batch/transform_fuel_transactions.py`
- Read Bronze Parquet
- Most complex: dedup, timestamp parse, weekend flag, maintenance JOIN, LAG window for distance, km/L calc → write Delta to Silver

#### [MODIFY] `dags/omniroute_daily_batch.py`
- Uncomment Silver TaskGroup, add 3 transform tasks (registry, assignment, fuel)
- Wire Bronze → Silver dependencies

#### [MODIFY] `dags/omniroute_yearly_batch.py`
- Uncomment Silver TaskGroup, add maintenance transform task
- Wire Bronze → Silver dependency

---

## Key Design Decisions

1. **Delta Lake format** for Silver (not plain Parquet) — provides ACID transactions, schema enforcement, and enables MERGE for Gold SCD2
2. **`overwrite` mode** for vehicle_registry and maintenance (full snapshots) vs **`append`/`merge`** for assignment and fuel (incremental)
3. **All paths from environment variables** with sensible defaults — consistent with Bronze pattern
4. **Every script follows the same structure**: read → validate → transform → write → log

## Verification Plan

### Automated Tests
- Each script has proper `argparse` with `--run-date` for testability
- Schema validation before write
- Row count logging for audit trail

### Manual Verification
- Verify DAGs parse correctly: `python dags/omniroute_daily_batch.py`
- Verify DAGs parse correctly: `python dags/omniroute_yearly_batch.py`
