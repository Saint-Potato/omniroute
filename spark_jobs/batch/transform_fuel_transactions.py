"""
Silver Transformation — Fuel Transactions (Incremental + Enriched)
===================================================================
Reads ONLY today's partition from Bronze `ingested/fuel_transactions`,
cleans it, adds exclusion flags (weekend, maintenance), and MERGEs it
into the existing Silver Delta Lake table.

Pipeline position:  Bronze → **Silver** → Gold (Fuel Efficiency Audit)
Upstream:           daily_ingest_fuel_transactions.py (Bronze ingestion)
                    transform_maintenance_schedules.py (Silver — needed for JOIN)
Downstream:         build_fuel_efficiency_audit.py (Gold — distance, km/L, 12% flagging)

⚠ DEPENDENCY: This script MUST run AFTER transform_maintenance_schedules.py
because it JOINs with silver.maintenance_schedules to flag maintenance days.

─────────────────────────────────────────────────────────────────────────
WHAT SILVER DOES (clean facts + exclusion flags)
─────────────────────────────────────────────────────────────────────────
Silver's job is to deliver CLEAN FACTS with simple derived columns:
  • Dedup by transaction_id
  • Parse timestamp string → Timestamp type
  • Derive txn_date from timestamp
  • Flag is_weekend (from day_of_week — single-row derivation)
  • Flag is_maintenance_day (JOIN with Silver maintenance)
  • Filter invalid fuel_liters (≤ 0)
  • Cast columns to proper types

─────────────────────────────────────────────────────────────────────────
WHAT SILVER DOES NOT DO (pushed to Gold layer)
─────────────────────────────────────────────────────────────────────────
The following are BUSINESS METRICS, not data cleaning operations.
They belong in the Gold layer (build_fuel_efficiency_audit.py):
  ✗ prev_odometer   — window function across historical data
  ✗ distance_km     — computed from prev_odometer (business concept)
  ✗ km_per_liter    — business KPI that feeds the 12% audit threshold

WHY? Because:
  1. These are business-defined calculations, not data quality operations.
  2. If the audit formula changes (e.g., from km/L to miles/gallon),
     only Gold needs to change — Silver stays stable.
  3. It eliminates the complex "seed row" strategy for incremental LAG()
     — Gold has access to the full Silver history and can compute LAG()
     efficiently using Delta Lake time travel or full table reads.

─────────────────────────────────────────────────────────────────────────
MERGE LOGIC (Delta Lake MERGE INTO)
─────────────────────────────────────────────────────────────────────────
Match key: transaction_id (globally unique per fuel event)

  WHEN MATCHED:
    → Do NOT update (same transaction should never change).
      This makes re-runs idempotent.

  WHEN NOT MATCHED:
    → INSERT the new cleaned fuel record.

Usage:
    spark-submit --packages io.delta:delta-spark_2.12:3.3.0 \\
        spark_jobs/batch/transform_fuel_transactions.py --run-date 2026-04-16
"""

import os
import argparse
from datetime import date

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, to_date, to_timestamp, row_number, current_timestamp,
    dayofweek, when, lit,
)
from pyspark.sql.types import FloatType

# Delta Lake MERGE support
from delta.tables import DeltaTable


# ──────────────────────────────────────────────
# S3 / Storage Paths
# ──────────────────────────────────────────────

# BRONZE_PATH — Bronze fuel transactions (Parquet, partitioned by load_date)
BRONZE_PATH = os.environ.get(
    "INGESTED_PATH", "s3a://omniroute-bronze/ingested/"
) + "fuel_transactions"

# PROCESSED_PATH — where we move Bronze partitions AFTER successful Silver merge.
PROCESSED_PATH = os.environ.get(
    "PROCESSED_PATH", "s3a://omniroute-bronze/processed/"
) + "fuel_transactions"

# SILVER_MAINTENANCE_PATH — Silver maintenance schedules (Delta)
# This is needed for the JOIN to flag maintenance days.
SILVER_MAINTENANCE_PATH = os.environ.get(
    "SILVER_PATH", "s3a://omniroute-data-lake/silver/"
) + "maintenance_schedules"

# SILVER_PATH — Output: cleaned fuel transactions (Delta)
SILVER_PATH = os.environ.get(
    "SILVER_PATH", "s3a://omniroute-data-lake/silver/"
) + "fuel_transactions_clean"


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def silver_table_exists(spark: SparkSession) -> bool:
    """Check whether the Silver Delta table already exists."""
    try:
        DeltaTable.forPath(spark, SILVER_PATH)
        return True
    except Exception:
        return False


def move_to_processed(spark: SparkSession, run_date: str):
    """Move today's Bronze partition from ingested/ → processed/.

    After a successful Silver transformation, the Bronze data that was just
    processed must be moved out of ingested/ so it won't be picked up again
    on the next run.

    Uses Hadoop FileSystem API (works on both S3/s3a and HDFS).
    On S3, 'rename' = copy + delete under the hood.
    """
    source = f"{BRONZE_PATH}/load_date={run_date}"
    target = f"{PROCESSED_PATH}/load_date={run_date}"

    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        source_path = spark._jvm.org.apache.hadoop.fs.Path(source)
        target_path = spark._jvm.org.apache.hadoop.fs.Path(target)
        fs = source_path.getFileSystem(hadoop_conf)

        if fs.exists(source_path):
            fs.mkdirs(target_path.getParent())
            success = fs.rename(source_path, target_path)
            if success:
                print(f"[silver.fuel_transactions_clean] ✓ Moved Bronze → processed: {target}")
            else:
                print(f"[silver.fuel_transactions_clean] ⚠ Move failed: {source}")
        else:
            print(f"[silver.fuel_transactions_clean] ⚠ Source not found (already moved?): {source}")

    except Exception as e:
        # Don't fail the whole job if the move fails — Silver data is already safe.
        print(f"[silver.fuel_transactions_clean] ⚠ Failed to move Bronze to processed: {e}")


def add_weekend_flags(df):
    """Add day_of_week and is_weekend columns.

    Spark's dayofweek() returns: 1 = Sunday, ..., 7 = Saturday.
    BRD Section 3.3.2: "Exclude weekends from fuel efficiency calculation."

    Edge case from producer: TXN_WEEKEND_TEST on 2026-05-17 (Sunday)
    → day_of_week = 1 → is_weekend = TRUE → excluded from Gold audit.
    """
    return (
        df
        .withColumn("day_of_week", dayofweek(col("txn_date")))
        .withColumn(
            "is_weekend",
            when(col("day_of_week").isin(1, 7), lit(True)).otherwise(lit(False))
        )
    )


def add_maintenance_flag(spark, df):
    """LEFT JOIN with silver.maintenance_schedules to flag maintenance days.

    BRD Section 3.3.2: "If a vehicle is listed in maintenance_schedules
    for that day, that day's fuel data MUST be excluded."

    Edge case from producer: TXN_MAINT_TEST on 2026-05-10 where vins[0]
    has a scheduled "Engine Overhaul" → is_maintenance_day = TRUE.

    We use LEFT JOIN so fuel transactions WITHOUT a matching maintenance
    record keep is_maintenance_day = FALSE (the majority of records).
    """
    try:
        maintenance_df = (
            spark.read
            .format("delta")
            .load(SILVER_MAINTENANCE_PATH)
            .filter(col("is_active") == True)  # Only active (not soft-deleted) maintenance events  # noqa: E712
            .select(
                col("vin").alias("maint_vin"),
                col("service_date").alias("maint_date"),
            )
            .distinct()  # One flag per (vin, date) is enough
        )

        df = (
            df
            .join(
                maintenance_df,
                (col("vin") == col("maint_vin"))
                & (col("txn_date") == col("maint_date")),
                how="left"
            )
            .withColumn(
                "is_maintenance_day",
                when(col("maint_vin").isNotNull(), lit(True)).otherwise(lit(False))
            )
            .drop("maint_vin", "maint_date")
        )
        print("[silver.fuel_transactions_clean] ✓ Joined with maintenance schedules")
        return df

    except Exception as e:
        # If maintenance Silver table doesn't exist yet (first run, or
        # yearly DAG hasn't run), default to is_maintenance_day = FALSE.
        print(f"[silver.fuel_transactions_clean] ⚠ Could not read maintenance Silver: {e}")
        print("[silver.fuel_transactions_clean] Defaulting is_maintenance_day = FALSE")
        return df.withColumn("is_maintenance_day", lit(False))


# ──────────────────────────────────────────────
# Main Transformation Logic
# ──────────────────────────────────────────────

def run(spark: SparkSession, run_date: str):
    """
    Execute the INCREMENTAL Silver transformation for fuel_transactions.

    Silver delivers:
      • Clean facts: transaction_id, vin, fuel_liters, odometer_reading, timestamp
      • Derived dates: txn_date, day_of_week
      • Exclusion flags: is_weekend, is_maintenance_day

    Gold will then compute:
      • prev_odometer (LAG over full Silver history)
      • distance_km (odometer - prev_odometer)
      • km_per_liter (distance / fuel)
      • FLAGGED/OK (12% threshold)
    """
    print(f"[silver.fuel_transactions_clean] run_date={run_date}")
    print(f"[silver.fuel_transactions_clean] Reading Bronze partition: load_date={run_date}")

    # ── Step 1: Read ONLY today's Bronze partition ──────────────────
    bronze_partition_path = f"{BRONZE_PATH}/load_date={run_date}"

    try:
        bronze_df = spark.read.parquet(bronze_partition_path)
    except Exception as e:
        print(f"[silver.fuel_transactions_clean] ⚠ No Bronze partition for {run_date}: {e}")
        print("[silver.fuel_transactions_clean] Nothing to process. Exiting.")
        return

    total_bronze_rows = bronze_df.count()
    print(f"[silver.fuel_transactions_clean] Bronze rows read: {total_bronze_rows}")

    if total_bronze_rows == 0:
        print("[silver.fuel_transactions_clean] ⚠ Empty partition. Skipping.")
        return

    # ── Step 2: Drop records with NULL keys ─────────────────────────
    df = bronze_df.filter(
        col("transaction_id").isNotNull()
        & (col("transaction_id") != "")
        & col("vin").isNotNull()
        & (col("vin") != "")
    )
    dropped_nulls = total_bronze_rows - df.count()
    if dropped_nulls > 0:
        print(f"[silver.fuel_transactions_clean] Dropped {dropped_nulls} rows with NULL keys")

    # ── Step 3: Parse timestamp string → Timestamp type ─────────────
    # Bronze stores timestamp as String ("2026-04-14 06:45:00").
    # We convert to proper Spark TimestampType for date functions.
    df = df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
    )
    df = df.filter(col("timestamp").isNotNull())

    # ── Step 4: Deduplicate WITHIN today's batch by transaction_id ──
    # Producer injects ~1% exact duplicate rows.
    dedup_window = Window.partitionBy("transaction_id").orderBy(col("timestamp"))
    df = (
        df
        .withColumn("_rn", row_number().over(dedup_window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
    print(f"[silver.fuel_transactions_clean] After dedup: {df.count()} rows")

    # ── Step 5: Derive txn_date from timestamp ──────────────────────
    df = df.withColumn("txn_date", to_date(col("timestamp")))

    # ── Step 6: Add weekend flags ───────────────────────────────────
    df = add_weekend_flags(df)

    # ── Step 7: Add maintenance day flag (JOIN with Silver maintenance)
    df = add_maintenance_flag(spark, df)

    # ── Step 8: Filter out invalid fuel_liters ──────────────────────
    # Edge cases: fuel_liters = 0 (div-by-zero in Gold), fuel_liters = -10 (impossible)
    df = df.filter(
        col("fuel_liters").cast(FloatType()).isNotNull()
        & (col("fuel_liters").cast(FloatType()) > 0)
    )

    # ── Step 9: Select final Silver columns ─────────────────────────
    # Silver stores CLEAN FACTS + exclusion flags.
    # Notably ABSENT: prev_odometer, distance_km, km_per_liter
    # → these are business metrics computed in the Gold layer.
    df = df.select(
        "transaction_id",
        "vin",
        col("fuel_liters").cast(FloatType()).alias("fuel_liters"),
        col("odometer_reading").cast(FloatType()).alias("odometer_reading"),
        "timestamp",
        "txn_date",
        "day_of_week",
        "is_weekend",
        "is_maintenance_day",
    ).withColumn("_silver_processed_at", current_timestamp())

    incoming_count = df.count()
    print(f"[silver.fuel_transactions_clean] Incoming records to write: {incoming_count}")

    if incoming_count == 0:
        print("[silver.fuel_transactions_clean] ⚠ All rows filtered out. Skipping.")
        return

    # ── Step 10: MERGE or Bootstrap ─────────────────────────────────
    if silver_table_exists(spark):
        # ──────────────────────────────────────────────────────
        # INCREMENTAL MERGE — insert new, skip existing
        # ──────────────────────────────────────────────────────
        # Match on transaction_id (globally unique per fuel event).
        # WHEN MATCHED → do nothing (same transaction shouldn't change).
        # WHEN NOT MATCHED → insert the cleaned record.
        # This makes re-runs idempotent.
        print("[silver.fuel_transactions_clean] Silver table exists → Delta MERGE")

        silver_table = DeltaTable.forPath(spark, SILVER_PATH)

        (
            silver_table.alias("existing")
            .merge(
                df.alias("incoming"),
                "existing.transaction_id = incoming.transaction_id"
            )
            # No whenMatchedUpdate — we DON'T overwrite existing transactions.
            # This is intentional: idempotency means "run twice, get same result."
            .whenNotMatchedInsertAll()
            .execute()
        )

        final_count = spark.read.format("delta").load(SILVER_PATH).count()
        print(f"[silver.fuel_transactions_clean] ✓ MERGE complete. Total Silver rows: {final_count}")

    else:
        # ──────────────────────────────────────────────────────
        # FIRST RUN (Bootstrap) — create the Delta table
        # ──────────────────────────────────────────────────────
        print("[silver.fuel_transactions_clean] Silver table does NOT exist → Bootstrap write")

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(SILVER_PATH)
        )

        print(f"[silver.fuel_transactions_clean] ✓ Bootstrap: wrote {incoming_count} rows → {SILVER_PATH}")

    # ── Post-MERGE: Move Bronze partition to processed/ ─────────
    # This MUST happen AFTER a successful write/merge so we don't
    # lose data if the transformation failed midway.
    move_to_processed(spark, run_date)


# ──────────────────────────────────────────────
# Entry Point
# ──────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Silver transformation for fuel_transactions (incremental)"
    )
    parser.add_argument(
        "--run-date",
        default=str(date.today()),
        help="Logical execution date (YYYY-MM-DD). Determines which Bronze partition to read.",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("OmniRoute_transform_fuel_transactions")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    try:
        run(spark, args.run_date)
    finally:
        spark.stop()
