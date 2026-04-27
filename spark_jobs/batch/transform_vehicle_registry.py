"""
Silver Transformation — Vehicle Registry (SCD Type 1 via Delta MERGE)
=======================================================================
Reads the latest Bronze `ingested/vehicle_registry` Parquet snapshot,
cleans it, and MERGEs it into the Silver Delta table — preserving history
instead of blindly overwriting.

Pipeline position:  Bronze → **Silver** → Gold
Upstream:           daily_ingest_vehicle_registry.py (Bronze ingestion)
Downstream:         build_asset_history_scd2.py, build_fuel_efficiency_audit.py (Gold)

─────────────────────────────────────────────────────────────────────────
WHY MERGE INSTEAD OF OVERWRITE?
─────────────────────────────────────────────────────────────────────────
Vehicle Registry is a daily FULL SNAPSHOT — every day, the entire list of
active vehicles is delivered. A naive approach would overwrite Silver every
time, but this DESTROYS HISTORY:
  • We lose decommissioned vehicles (removed from source but needed for
    audit trail, historical JOINs, and compliance queries).
  • We lose "when did this vehicle's data change?" lineage.

Instead, we use Delta Lake's MERGE (SCD Type 1 with soft-delete):

  WHEN MATCHED AND data changed:
    → UPDATE the row with new values (model, fuel_type, mfg_year).
      This is SCD Type 1: "just overwrite the changed attributes."

  WHEN MATCHED AND data is identical:
    → No-op (skip). Saves write amplification.

  WHEN NOT MATCHED (new VIN in source):
    → INSERT with is_active = TRUE.

  WHEN NOT MATCHED BY SOURCE (VIN in Silver but NOT in today's snapshot):
    → SOFT-DELETE: set is_active = FALSE instead of hard-deleting.
      This means decommissioned vehicles stay in Silver for audit queries.
      BRD/compliance teams can still query historical fleet composition.

─────────────────────────────────────────────────────────────────────────
DELTA TIME TRAVEL (Bonus)
─────────────────────────────────────────────────────────────────────────
Because we MERGE (not overwrite), Delta Lake keeps a transaction log of
every change. You can query ANY past version:
  spark.read.format("delta").option("versionAsOf", 5).load(SILVER_PATH)
  spark.read.format("delta").option("timestampAsOf", "2026-04-10").load(SILVER_PATH)

Usage:
    spark-submit --packages io.delta:delta-spark_2.12:3.3.0 \\
        spark_jobs/batch/transform_vehicle_registry.py --run-date 2026-04-16
"""

import os
import argparse
from datetime import date, datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, trim, upper, row_number, lit, current_timestamp,
)
from pyspark.sql.types import IntegerType

# Delta Lake MERGE support
from delta.tables import DeltaTable


# ──────────────────────────────────────────────
# S3 / Storage Paths
# ──────────────────────────────────────────────

# BRONZE_PATH — where the Bronze ingestion job writes Parquet, partitioned by load_date
BRONZE_PATH = os.environ.get(
    "INGESTED_PATH", "s3a://omniroute-bronze/ingested/"
) + "vehicle_registry"

# PROCESSED_PATH — where we move Bronze data AFTER successful Silver merge.
# For a full-snapshot table, we move ALL partitions from ingested/ to processed/.
PROCESSED_PATH = os.environ.get(
    "PROCESSED_PATH", "s3a://omniroute-bronze/processed/"
) + "vehicle_registry"

# SILVER_PATH — where this script writes/merges the cleaned Delta Lake table
SILVER_PATH = os.environ.get(
    "SILVER_PATH", "s3a://omniroute-data-lake/silver/"
) + "vehicle_registry_clean"

# Known valid fuel types per BRD / config.py
VALID_FUEL_TYPES = {"DIESEL", "LNG", "CNG", "ELECTRIC"}

# Reasonable manufacturing year range
CURRENT_YEAR = datetime.utcnow().year
MIN_MFG_YEAR = 2000
MAX_MFG_YEAR = CURRENT_YEAR + 1  # Allow next year for pre-registered vehicles


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


def move_all_to_processed(spark: SparkSession, run_date: str):
    """Move all Bronze ingested partitions to processed/ after successful merge.

    Vehicle Registry is a FULL SNAPSHOT — we read ALL partitions for dedup.
    After processing, we move them all to processed/ so the next run only
    finds the new day's snapshot in ingested/.

    Uses Hadoop FileSystem API (works on both S3/s3a and HDFS).
    """
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        source_path = spark._jvm.org.apache.hadoop.fs.Path(BRONZE_PATH)
        fs = source_path.getFileSystem(hadoop_conf)

        if not fs.exists(source_path):
            print(f"[silver.vehicle_registry_clean] ⚠ Bronze path not found: {BRONZE_PATH}")
            return

        # List all load_date= partition directories inside ingested/vehicle_registry/
        status_list = fs.listStatus(source_path)
        moved_count = 0

        for status in status_list:
            partition_path = status.getPath()
            partition_name = partition_path.getName()  # e.g., "load_date=2026-04-16"

            target = spark._jvm.org.apache.hadoop.fs.Path(
                f"{PROCESSED_PATH}/{partition_name}"
            )
            fs.mkdirs(target.getParent())
            success = fs.rename(partition_path, target)
            if success:
                moved_count += 1

        print(f"[silver.vehicle_registry_clean] ✓ Moved {moved_count} Bronze partition(s) → processed/")

    except Exception as e:
        # Don't fail the job if move fails — Silver data is already safe.
        print(f"[silver.vehicle_registry_clean] ⚠ Failed to move Bronze to processed: {e}")


def clean_snapshot(bronze_df):
    """Apply all data quality rules to the incoming Bronze snapshot.
    
    This function encapsulates the cleaning logic so it can be used for
    both first-run (bootstrap) and subsequent runs (MERGE source).
    
    Returns a cleaned DataFrame with the Silver schema.
    """

    # ── Drop records with NULL VIN ──────────────────────────────────
    # VIN is our primary key — records without it are useless.
    df = bronze_df.filter(col("vin").isNotNull() & (col("vin") != ""))

    # ── Deduplicate by VIN within today's snapshot ──────────────────
    # If the same VIN appears in multiple partitions, keep the latest.
    # Edge case from producer: "The Clones" — exact duplicate rows.
    dedup_window = Window.partitionBy("vin").orderBy(col("load_date").desc())

    df = (
        df
        .withColumn("_rn", row_number().over(dedup_window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    # ── Clean string columns ────────────────────────────────────────
    # TRIM + UPPER: "diesel", " Diesel ", "DIESEL" all become "DIESEL".
    df = (
        df
        .withColumn("model", trim(upper(col("model"))))
        .withColumn("fuel_type", trim(upper(col("fuel_type"))))
    )

    # ── Validate fuel_type ──────────────────────────────────────────
    # Edge case from producer: "INVALID_FUEL" → filtered out here.
    df = df.filter(col("fuel_type").isin(list(VALID_FUEL_TYPES)))

    # ── Validate mfg_year ───────────────────────────────────────────
    # Edge case from producer: empty mfg_year → NULL after cast → filtered.
    df = (
        df
        .withColumn("mfg_year", col("mfg_year").cast(IntegerType()))
        .filter(
            col("mfg_year").isNotNull()
            & (col("mfg_year") >= MIN_MFG_YEAR)
            & (col("mfg_year") <= MAX_MFG_YEAR)
        )
    )

    # ── Select final Silver columns ─────────────────────────────────
    # Drop Bronze metadata, add Silver audit columns.
    df = df.select(
        "vin",
        "model",
        "mfg_year",
        "fuel_type",
    ).withColumn("is_active", lit(True))  # All vehicles in snapshot are active

    return df


# ──────────────────────────────────────────────
# Main Transformation Logic
# ──────────────────────────────────────────────

def run(spark: SparkSession, run_date: str):
    """
    Execute the Silver transformation for vehicle_registry.

    Strategy (SCD Type 1 + soft-delete via Delta MERGE):
      1. Read the full Bronze snapshot, clean it
      2. MERGE INTO existing Silver Delta table:
         - VIN exists AND data changed → UPDATE (SCD1)
         - VIN exists AND same data → SKIP (no write amplification)
         - New VIN → INSERT with is_active = TRUE
         - VIN in Silver but NOT in source → SOFT-DELETE (is_active = FALSE)
      3. On first run (no Silver table), do a direct write (bootstrap)
    """
    print(f"[silver.vehicle_registry_clean] run_date={run_date}")
    print(f"[silver.vehicle_registry_clean] Reading Bronze from: {BRONZE_PATH}")

    # ── Step 1: Read the full Bronze snapshot and clean it ──────────
    # Vehicle Registry is a daily FULL SNAPSHOT — we read all partitions
    # to get the complete current state of the fleet.
    bronze_df = spark.read.parquet(BRONZE_PATH)

    total_bronze_rows = bronze_df.count()
    print(f"[silver.vehicle_registry_clean] Bronze rows read: {total_bronze_rows}")

    if total_bronze_rows == 0:
        print("[silver.vehicle_registry_clean] ⚠ No data in Bronze. Skipping.")
        return

    incoming_df = clean_snapshot(bronze_df)
    incoming_count = incoming_df.count()
    print(f"[silver.vehicle_registry_clean] After cleaning: {incoming_count} rows")

    if incoming_count == 0:
        print("[silver.vehicle_registry_clean] ⚠ All rows filtered out. Skipping.")
        return

    # ── Step 2: MERGE or Bootstrap ──────────────────────────────────
    if silver_table_exists(spark):
        # ──────────────────────────────────────────────────────
        # SCD TYPE 1 MERGE + SOFT-DELETE
        # ──────────────────────────────────────────────────────
        # This is the core logic that preserves history:
        #
        # MATCH on: vin (primary key of vehicle registry)
        #
        # Case 1: VIN exists in both source and Silver, data CHANGED
        #   → UPDATE: overwrite model/mfg_year/fuel_type (SCD Type 1)
        #     Also re-activate if it was previously soft-deleted.
        #
        # Case 2: VIN exists in both, data IDENTICAL
        #   → SKIP: no-op. Delta's merge naturally skips rows where
        #     the update condition is not met.
        #
        # Case 3: VIN in source but NOT in Silver (new vehicle)
        #   → INSERT with is_active = TRUE.
        #
        # Case 4: VIN in Silver but NOT in today's source snapshot
        #   → SOFT-DELETE: set is_active = FALSE.
        #     The vehicle was removed from the registry (decommissioned,
        #     retired, or transferred out). We KEEP the row so:
        #     • Historical JOINs (Gold SCD2) still resolve this VIN.
        #     • Compliance audits can query past fleet composition.
        #     • Delta time travel shows when it was deactivated.
        print("[silver.vehicle_registry_clean] Silver table exists → Delta MERGE (SCD1 + soft-delete)")

        silver_table = DeltaTable.forPath(spark, SILVER_PATH)

        (
            silver_table.alias("existing")
            .merge(
                incoming_df.alias("incoming"),
                "existing.vin = incoming.vin"
            )
            # Case 1: Data changed → update (including re-activate)
            .whenMatchedUpdate(
                condition="""
                    existing.model != incoming.model
                    OR existing.mfg_year != incoming.mfg_year
                    OR existing.fuel_type != incoming.fuel_type
                    OR existing.is_active = FALSE
                """,
                set={
                    "model":                 "incoming.model",
                    "mfg_year":              "incoming.mfg_year",
                    "fuel_type":             "incoming.fuel_type",
                    "is_active":             "incoming.is_active",
                    "_silver_processed_at":   lit(current_timestamp()),
                }
            )
            # Case 3: New VIN → insert
            .whenNotMatchedInsert(
                values={
                    "vin":                   "incoming.vin",
                    "model":                 "incoming.model",
                    "mfg_year":              "incoming.mfg_year",
                    "fuel_type":             "incoming.fuel_type",
                    "is_active":             "incoming.is_active",
                    "_silver_processed_at":   lit(current_timestamp()),
                }
            )
            # Case 4: VIN gone from source → soft-delete (only if currently active)
            .whenNotMatchedBySourceUpdate(
                condition="existing.is_active = TRUE",
                set={
                    "is_active":             lit(False),
                    "_silver_processed_at":   lit(current_timestamp()),
                }
            )
            .execute()
        )

        # Report stats
        final_df = spark.read.format("delta").load(SILVER_PATH)
        active_count = final_df.filter(col("is_active") == True).count()  # noqa: E712
        inactive_count = final_df.filter(col("is_active") == False).count()  # noqa: E712
        print(f"[silver.vehicle_registry_clean] ✓ MERGE complete.")
        print(f"  Active vehicles:   {active_count}")
        print(f"  Inactive (soft-deleted): {inactive_count}")
        print(f"  Total Silver rows: {active_count + inactive_count}")

    else:
        # ──────────────────────────────────────────────────────
        # FIRST RUN (Bootstrap)
        # ──────────────────────────────────────────────────────
        print("[silver.vehicle_registry_clean] Silver table does NOT exist → Bootstrap write")

        incoming_df = incoming_df.withColumn("_silver_processed_at", current_timestamp())

        (
            incoming_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(SILVER_PATH)
        )

        print(f"[silver.vehicle_registry_clean] ✓ Bootstrap: wrote {incoming_count} rows → {SILVER_PATH}")

    # ── Post-MERGE: Move ALL Bronze partitions to processed/ ──────
    # Since registry reads ALL partitions (full snapshot), we move them all.
    # This MUST happen AFTER a successful write/merge.
    move_all_to_processed(spark, run_date)


# ──────────────────────────────────────────────
# Entry Point
# ──────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Silver transformation for vehicle_registry (SCD1 + soft-delete)"
    )
    parser.add_argument(
        "--run-date",
        default=str(date.today()),
        help="Logical execution date (YYYY-MM-DD)",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("OmniRoute_transform_vehicle_registry")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    try:
        run(spark, args.run_date)
    finally:
        spark.stop()
