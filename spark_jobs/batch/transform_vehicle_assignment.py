"""
Silver Transformation — Vehicle Assignment (Incremental + Status Tracking)
============================================================================
Reads ONLY today's partition from Bronze `ingested/vehicle_assignment`,
cleans it, derives assignment status, and MERGEs it into the Silver Delta table.

Pipeline position:  Bronze → **Silver** → Gold (SCD Type 2)
Upstream:           daily_ingest_vehicle_assignment.py (Bronze ingestion)
Downstream:         build_asset_history_scd2.py (Gold — accumulated SCD2 history)

─────────────────────────────────────────────────────────────────────────
SILVER'S ROLE vs GOLD'S ROLE
─────────────────────────────────────────────────────────────────────────
What Silver does:
  • Clean, validate, dedup each incoming batch
  • Convert Unix timestamps → Date
  • Resolve conflicts (highest daily_rate wins per vin+start_date)
  • Derive `status` and `_is_current` from end_date:
      end_date IS NULL  → status = 'IN-TRANSIT', _is_current = TRUE
      end_date IS NOT NULL → status = 'ARCHIVED', _is_current = FALSE
  • Handle the driver swap: when an existing record's end_date changes
    from NULL to a real date (driver was replaced), UPDATE Silver to
    reflect the closed assignment.

What Gold does (downstream, NOT this script):
  • Build the ACCUMULATED SCD2 history table (gold.asset_history_scd2)
  • That table grows over time, never loses rows
  • Gold handles the case where ONLY a new record arrives (without
    the old record's end_date being updated by the source) by detecting
    "new assignment for same VIN → close the old one"

─────────────────────────────────────────────────────────────────────────
MERGE LOGIC (Delta Lake MERGE INTO)
─────────────────────────────────────────────────────────────────────────
Match key: (vin, start_date)

  WHEN MATCHED AND any data changed:
    → UPDATE the row. This covers:
      • daily_rate changed (conflict resolution / rate correction)
      • end_date changed from NULL to a date (DRIVER SWAP — the old
        driver was replaced, so this assignment is now ARCHIVED)
      • driver_id changed (assignment correction)

  WHEN NOT MATCHED:
    → INSERT the new assignment record

─────────────────────────────────────────────────────────────────────────
DRIVER SWAP EXAMPLE (end-to-end through Silver)
─────────────────────────────────────────────────────────────────────────
Day 1:  Source sends → VIN-SWAP-TEST, DRV_SWAP_1, start=Apr 1, end=NULL
        Silver MERGE → NOT MATCHED → INSERT
        Silver state: | VIN-SWAP-TEST | DRV_SWAP_1 | Apr 1 | NULL   | IN-TRANSIT | TRUE |

Day 15: Source sends two records:
        Record A → VIN-SWAP-TEST, DRV_SWAP_1, start=Apr 1, end=Apr 15
        Record B → VIN-SWAP-TEST, DRV_SWAP_2, start=Apr 15, end=NULL

        Silver MERGE for Record A:
          → MATCHED on (VIN-SWAP-TEST, Apr 1)
          → end_date changed: NULL → Apr 15  → UPDATE
          Silver state: | VIN-SWAP-TEST | DRV_SWAP_1 | Apr 1  | Apr 15 | ARCHIVED   | FALSE |

        Silver MERGE for Record B:
          → NOT MATCHED on (VIN-SWAP-TEST, Apr 15)  → INSERT
          Silver state: | VIN-SWAP-TEST | DRV_SWAP_2 | Apr 15 | NULL   | IN-TRANSIT | TRUE  |

Gold can now read Silver and clearly see: who was on this vehicle before, who is on it now.

Usage:
    spark-submit --packages io.delta:delta-spark_2.12:3.3.0 \\
        spark_jobs/batch/transform_vehicle_assignment.py --run-date 2026-04-16
"""

import os
import argparse
from datetime import date

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, trim, upper, row_number, from_unixtime, to_date,
    current_timestamp, when, lit,
)
from pyspark.sql.types import LongType, FloatType

# Delta Lake MERGE support
from delta.tables import DeltaTable


# ──────────────────────────────────────────────
# S3 / Storage Paths
# ──────────────────────────────────────────────

# BRONZE_PATH — where the Bronze ingestion job writes Parquet (partitioned by load_date)
BRONZE_PATH = os.environ.get(
    "INGESTED_PATH", "s3a://omniroute-bronze/ingested/"
) + "vehicle_assignment"

# PROCESSED_PATH — where we move Bronze partitions AFTER successful Silver merge.
# This prevents the same data from being re-processed on the next run.
# Structure mirrors ingested/: processed/vehicle_assignment/load_date=YYYY-MM-DD/
PROCESSED_PATH = os.environ.get(
    "PROCESSED_PATH", "s3a://omniroute-bronze/processed/"
) + "vehicle_assignment"

# SILVER_PATH — where this script writes/merges the cleaned Delta table
SILVER_PATH = os.environ.get(
    "SILVER_PATH", "s3a://omniroute-data-lake/silver/"
) + "vehicle_assignment_clean"


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
    On S3, 'rename' = copy + delete under the hood (atomic within S3).
    """
    source = f"{BRONZE_PATH}/load_date={run_date}"
    target = f"{PROCESSED_PATH}/load_date={run_date}"

    try:
        # Get Hadoop FileSystem handle via Spark's JVM bridge
        hadoop_conf = spark._jsc.hadoopConfiguration()
        source_path = spark._jvm.org.apache.hadoop.fs.Path(source)
        target_path = spark._jvm.org.apache.hadoop.fs.Path(target)
        fs = source_path.getFileSystem(hadoop_conf)

        if fs.exists(source_path):
            # Ensure parent directory exists in processed/
            fs.mkdirs(target_path.getParent())
            # Move (rename) the entire partition directory
            success = fs.rename(source_path, target_path)
            if success:
                print(f"[silver.vehicle_assignment_clean] ✓ Moved Bronze → processed: {target}")
            else:
                print(f"[silver.vehicle_assignment_clean] ⚠ Move failed (rename returned false): {source}")
        else:
            print(f"[silver.vehicle_assignment_clean] ⚠ Source partition not found (already moved?): {source}")

    except Exception as e:
        # Don't fail the whole job if the move fails — Silver data is already safe.
        # Log the error so ops can investigate and manually move if needed.
        print(f"[silver.vehicle_assignment_clean] ⚠ Failed to move Bronze to processed: {e}")
        print(f"[silver.vehicle_assignment_clean]   Source: {source}")
        print(f"[silver.vehicle_assignment_clean]   Target: {target}")


def clean_incoming_batch(bronze_df):
    """Apply all data quality rules to a batch of Bronze assignment records.
    
    Returns a cleaned DataFrame with the Silver schema, including derived
    status and _is_current columns.
    """

    # ── Drop records with NULL vin or NULL driver_id ────────────────
    # Both fields are required — a record without either is meaningless.
    df = bronze_df.filter(
        col("vin").isNotNull()
        & (col("vin") != "")
        & col("driver_id").isNotNull()
        & (col("driver_id") != "")
    )

    # ── Convert Unix timestamps → Date ─────────────────────────────
    # BRD Section 3.2.2: "Convert Unix timestamps to date format."
    #
    # start_timestamp (Long) → start_date (Date)
    # end_timestamp (Long or NULL) → end_date (Date or NULL)
    #
    # end_timestamp = NULL means "currently active assignment"
    df = (
        df
        .withColumn(
            "start_date",
            to_date(from_unixtime(col("start_timestamp").cast(LongType())))
        )
        .withColumn(
            "end_date",
            when(
                col("end_timestamp").isNotNull(),
                to_date(from_unixtime(col("end_timestamp").cast(LongType())))
            ).otherwise(lit(None).cast("date"))
        )
    )

    # ── Validate start_date and daily_rate ──────────────────────────
    df = df.filter(
        col("start_date").isNotNull()
        & (col("daily_rate").cast(FloatType()) > 0)
    )

    # ── Deduplicate WITHIN this batch ───────────────────────────────
    # BRD Conflict Resolution: same VIN + same start_date → keep highest daily_rate.
    # Handles VIN-CONFLICT-TEST edge case ($400 vs $600 → $600 wins).
    dedup_window = Window.partitionBy("vin", "start_date").orderBy(
        col("daily_rate").desc()
    )

    df = (
        df
        .withColumn("_rn", row_number().over(dedup_window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    # ── Clean region column ─────────────────────────────────────────
    df = df.withColumn("region", trim(upper(col("region"))))

    # ── Derive status and _is_current from end_date ─────────────────
    # This is a SIMPLE DERIVATION, not business logic:
    #   end_date IS NULL     → driver is CURRENTLY on this vehicle  → IN-TRANSIT
    #   end_date IS NOT NULL → driver was REPLACED or assignment ended → ARCHIVED
    #
    # Why derive this in Silver (not Gold)?
    #   Because every downstream consumer (Gold SCD2, streaming JOINs,
    #   reporting queries) needs to know "who is the current driver?"
    #   If we don't derive it here, every consumer must repeat the same
    #   CASE WHEN logic — violating DRY principle.
    df = (
        df
        .withColumn(
            "status",
            when(col("end_date").isNull(), lit("IN-TRANSIT"))
            .otherwise(lit("ARCHIVED"))
        )
        .withColumn(
            "_is_current",
            col("end_date").isNull()  # TRUE if end_date is NULL
        )
    )

    # ── Select final Silver columns ─────────────────────────────────
    df = df.select(
        "vin",
        "driver_id",
        "start_date",
        "end_date",
        col("daily_rate").cast(FloatType()).alias("daily_rate"),
        "region",
        "status",
        "_is_current",
    ).withColumn("_silver_processed_at", current_timestamp())

    return df


# ──────────────────────────────────────────────
# Main Transformation Logic
# ──────────────────────────────────────────────

def run(spark: SparkSession, run_date: str):
    """
    Execute the INCREMENTAL Silver transformation for vehicle_assignment.

    Strategy:
      1. Read ONLY today's Bronze partition (load_date = run_date)
      2. Clean, validate, dedup, derive status
      3. MERGE INTO existing Silver:
         - Match on (vin, start_date)
         - Update if ANY data changed (daily_rate, end_date, driver_id)
         - Insert if new
      4. On first run, bootstrap the table
    """
    print(f"[silver.vehicle_assignment_clean] run_date={run_date}")
    print(f"[silver.vehicle_assignment_clean] Reading Bronze partition: load_date={run_date}")

    # ── Step 1: Read ONLY today's Bronze partition ──────────────────
    bronze_partition_path = f"{BRONZE_PATH}/load_date={run_date}"

    try:
        bronze_df = spark.read.parquet(bronze_partition_path)
    except Exception as e:
        print(f"[silver.vehicle_assignment_clean] ⚠ No Bronze partition for {run_date}: {e}")
        print("[silver.vehicle_assignment_clean] Nothing to process. Exiting.")
        return

    total_bronze_rows = bronze_df.count()
    print(f"[silver.vehicle_assignment_clean] Bronze rows read: {total_bronze_rows}")

    if total_bronze_rows == 0:
        print("[silver.vehicle_assignment_clean] ⚠ Empty partition. Skipping.")
        return

    # ── Step 2: Clean the incoming batch ────────────────────────────
    incoming_df = clean_incoming_batch(bronze_df)
    incoming_count = incoming_df.count()
    print(f"[silver.vehicle_assignment_clean] After cleaning: {incoming_count} rows")

    if incoming_count == 0:
        print("[silver.vehicle_assignment_clean] ⚠ All rows filtered out. Skipping.")
        return

    # ── Step 3: MERGE or Bootstrap ──────────────────────────────────
    if silver_table_exists(spark):
        # ──────────────────────────────────────────────────────
        # INCREMENTAL MERGE — Delta Lake's ACID merge
        # ──────────────────────────────────────────────────────
        # Match key: (vin, start_date)
        #
        # WHEN MATCHED AND data changed:
        #   → UPDATE. This covers THREE scenarios:
        #
        #   1. DRIVER SWAP (end_date changed):
        #      Old record's end_date goes from NULL → 2026-04-15.
        #      status flips from IN-TRANSIT → ARCHIVED.
        #      _is_current flips from TRUE → FALSE.
        #      This is the CRITICAL fix — without this, the swap is lost.
        #
        #   2. RATE CORRECTION (daily_rate changed):
        #      A higher-paying record replaces a lower-paying one.
        #      BRD Conflict Resolution.
        #
        #   3. DRIVER REASSIGNMENT (driver_id changed):
        #      Same VIN+start_date but a different driver assigned.
        #      Edge case: admin correction.
        #
        # WHEN NOT MATCHED:
        #   → INSERT. New assignment that didn't exist before.
        #     e.g., DRV_SWAP_2 starting on Apr 15.
        print("[silver.vehicle_assignment_clean] Silver table exists → Delta MERGE")

        silver_table = DeltaTable.forPath(spark, SILVER_PATH)

        (
            silver_table.alias("existing")
            .merge(
                incoming_df.alias("incoming"),
                "existing.vin = incoming.vin AND existing.start_date = incoming.start_date"
            )
            .whenMatchedUpdate(
                # Update if ANY meaningful field changed.
                # The key change vs previous version: we now detect end_date changes
                # (driver swap) and driver_id changes (reassignment), not just daily_rate.
                condition="""
                    incoming.daily_rate != existing.daily_rate
                    OR incoming.driver_id != existing.driver_id
                    OR (incoming.end_date IS NOT NULL AND existing.end_date IS NULL)
                    OR (incoming.end_date IS NULL AND existing.end_date IS NOT NULL)
                    OR (incoming.end_date != existing.end_date)
                """,
                set={
                    "driver_id":             "incoming.driver_id",
                    "end_date":              "incoming.end_date",
                    "daily_rate":            "incoming.daily_rate",
                    "region":                "incoming.region",
                    "status":                "incoming.status",
                    "_is_current":           "incoming._is_current",
                    "_silver_processed_at":   "incoming._silver_processed_at",
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        # Report stats
        final_df = spark.read.format("delta").load(SILVER_PATH)
        active = final_df.filter(col("_is_current") == True).count()    # noqa: E712
        archived = final_df.filter(col("_is_current") == False).count()  # noqa: E712
        print(f"[silver.vehicle_assignment_clean] ✓ MERGE complete.")
        print(f"  IN-TRANSIT (current): {active}")
        print(f"  ARCHIVED (closed):    {archived}")
        print(f"  Total Silver rows:    {active + archived}")

    else:
        # ──────────────────────────────────────────────────────
        # FIRST RUN (Bootstrap)
        # ──────────────────────────────────────────────────────
        print("[silver.vehicle_assignment_clean] Silver table does NOT exist → Bootstrap write")

        (
            incoming_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(SILVER_PATH)
        )

        print(f"[silver.vehicle_assignment_clean] ✓ Bootstrap: wrote {incoming_count} rows → {SILVER_PATH}")

    # ── Post-MERGE: Move Bronze partition to processed/ ─────────
    # This MUST happen AFTER a successful write/merge so we don't
    # lose data if the transformation failed midway.
    move_to_processed(spark, run_date)


# ──────────────────────────────────────────────
# Entry Point
# ──────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Silver transformation for vehicle_assignment (incremental + status)"
    )
    parser.add_argument(
        "--run-date",
        default=str(date.today()),
        help="Logical execution date (YYYY-MM-DD)",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("OmniRoute_transform_vehicle_assignment")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    try:
        run(spark, args.run_date)
    finally:
        spark.stop()
