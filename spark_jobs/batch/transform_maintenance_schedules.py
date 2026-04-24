"""
Silver Transformation — Maintenance Schedules (Delta MERGE)
=============================================================
Reads the Bronze `ingested/maintenance_schedules` Parquet, cleans it,
and MERGEs into the Silver Delta table — preserving past maintenance
records instead of blindly overwriting.

Pipeline position:  Bronze → **Silver** → Gold (Fuel Efficiency Audit)
Upstream:           yearly_ingest_maintenance_schedules.py (Bronze ingestion)
Downstream:         transform_fuel_transactions.py (Silver — JOIN for is_maintenance_day)
                    build_fuel_efficiency_audit.py (Gold — exclusion logic)

This table is critical for fuel efficiency audit. The BRD explicitly states:
  "If a day falls on a weekend or the vehicle is listed in
   maintenance_schedules.csv for that day, that day's fuel data MUST
   be excluded to avoid penalizing drivers for workshop idling."
                                                — BRD Section 3.3.2

─────────────────────────────────────────────────────────────────────────
WHY MERGE INSTEAD OF OVERWRITE?
─────────────────────────────────────────────────────────────────────────
Maintenance is loaded once per YEAR (Jan 1st), but overwriting would:
  • Destroy last year's maintenance records. If the Gold fuel audit
    queries historical data spanning multiple years, it needs access
    to PAST maintenance schedules to correctly exclude those days.
  • Lose cancelled/rescheduled service records. If a service date that
    existed last year is removed from this year's file, we want to
    soft-delete it (mark is_active = FALSE), not erase it entirely.

MERGE strategy (same SCD1 + soft-delete as vehicle registry):
  WHEN MATCHED AND service_type changed → UPDATE (reschedule/correction)
  WHEN MATCHED AND same data           → SKIP
  WHEN NOT MATCHED (new service date)  → INSERT with is_active = TRUE
  WHEN NOT MATCHED BY SOURCE            → SOFT-DELETE (is_active = FALSE)

Usage:
    spark-submit --packages io.delta:delta-spark_2.12:3.3.0 \\
        spark_jobs/batch/transform_maintenance_schedules.py --run-date 2026-01-01
"""

import os
import argparse
from datetime import date

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, trim, to_date, row_number, current_timestamp, lit, when,
)

# Delta Lake MERGE support
from delta.tables import DeltaTable


# ──────────────────────────────────────────────
# S3 / Storage Paths
# ──────────────────────────────────────────────

# BRONZE_PATH — where the Bronze ingestion job writes Parquet
BRONZE_PATH = os.environ.get(
    "INGESTED_PATH", "s3a://omniroute-bronze/ingested/"
) + "maintenance_schedules"

# PROCESSED_PATH — where we move Bronze data AFTER successful Silver merge.
PROCESSED_PATH = os.environ.get(
    "PROCESSED_PATH", "s3a://omniroute-bronze/processed/"
) + "maintenance_schedules"

# SILVER_PATH — cleaned Delta table used by fuel transactions transformation
SILVER_PATH = os.environ.get(
    "SILVER_PATH", "s3a://omniroute-data-lake/silver/"
) + "maintenance_schedules"


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

    Maintenance is a yearly FULL LOAD — we read all partitions.
    After processing, move them to processed/.
    """
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        source_path = spark._jvm.org.apache.hadoop.fs.Path(BRONZE_PATH)
        fs = source_path.getFileSystem(hadoop_conf)

        if not fs.exists(source_path):
            print(f"[silver.maintenance_schedules] ⚠ Bronze path not found: {BRONZE_PATH}")
            return

        status_list = fs.listStatus(source_path)
        moved_count = 0

        for status in status_list:
            partition_path = status.getPath()
            partition_name = partition_path.getName()
            target = spark._jvm.org.apache.hadoop.fs.Path(
                f"{PROCESSED_PATH}/{partition_name}"
            )
            fs.mkdirs(target.getParent())
            success = fs.rename(partition_path, target)
            if success:
                moved_count += 1

        print(f"[silver.maintenance_schedules] ✓ Moved {moved_count} Bronze partition(s) → processed/")

    except Exception as e:
        print(f"[silver.maintenance_schedules] ⚠ Failed to move Bronze to processed: {e}")


def clean_snapshot(bronze_df):
    """Apply all data quality rules to the incoming Bronze maintenance data.
    
    Returns a cleaned DataFrame with the Silver schema.
    """

    # ── Parse service_date string → Date type ───────────────────────
    # The producer generates dates as "YYYY-MM-DD" strings.
    # Edge case: "INVALID_DATE" → to_date() returns NULL → filtered next.
    df = bronze_df.withColumn(
        "service_date",
        to_date(col("service_date"), "yyyy-MM-dd")
    )

    # ── Filter out rows with NULL/unparseable dates ─────────────────
    df = df.filter(col("service_date").isNotNull())

    # ── Filter out rows with NULL or INVALID_ VINs ──────────────────
    # Producer injects VINs like "INVALID_1234" — these don't exist in
    # the vehicle registry and would fail any downstream JOIN.
    df = df.filter(
        col("vin").isNotNull()
        & (col("vin") != "")
        & (~col("vin").startswith("INVALID_"))
    )

    # ── Deduplicate by (vin, service_date) ──────────────────────────
    # A vehicle can only have one "logical" maintenance event per day.
    dedup_window = Window.partitionBy("vin", "service_date").orderBy(
        col("service_type")  # Deterministic tiebreak: alphabetically first
    )

    df = (
        df
        .withColumn("_rn", row_number().over(dedup_window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    # ── Clean service_type ──────────────────────────────────────────
    # TRIM whitespace first, then handle the "Missing Information" edge case:
    # Producer generates records with completely blank service_type.
    # The maintenance DAY is still valid (vin + date exist), so we keep the
    # row but replace blank/null service_type with "UNKNOWN" for clarity.
    df = df.withColumn("service_type", trim(col("service_type")))
    df = df.withColumn(
        "service_type",
        when(
            col("service_type").isNull() | (col("service_type") == ""),
            lit("UNKNOWN")
        ).otherwise(col("service_type"))
    )

    # ── Select final Silver columns ─────────────────────────────────
    df = df.select(
        "vin",
        "service_date",
        "service_type",
    ).withColumn("is_active", lit(True))  # All records in current load are active

    return df


# ──────────────────────────────────────────────
# Main Transformation Logic
# ──────────────────────────────────────────────

def run(spark: SparkSession, run_date: str):
    """
    Execute the Silver transformation for maintenance_schedules.

    Strategy (SCD Type 1 + soft-delete via Delta MERGE):
      1. Read the Bronze snapshot, clean it
      2. MERGE INTO existing Silver Delta table:
         - (vin, service_date) exists AND service_type changed → UPDATE
         - (vin, service_date) exists AND same data → SKIP
         - New (vin, service_date) → INSERT with is_active = TRUE
         - (vin, service_date) in Silver but NOT in source → SOFT-DELETE
      3. On first run, do a direct write (bootstrap)
    """
    print(f"[silver.maintenance_schedules] run_date={run_date}")
    print(f"[silver.maintenance_schedules] Reading Bronze from: {BRONZE_PATH}")

    # ── Step 1: Read the full Bronze snapshot and clean it ──────────
    bronze_df = spark.read.parquet(BRONZE_PATH)

    total_bronze_rows = bronze_df.count()
    print(f"[silver.maintenance_schedules] Bronze rows read: {total_bronze_rows}")

    if total_bronze_rows == 0:
        print("[silver.maintenance_schedules] ⚠ No data in Bronze. Skipping.")
        return

    incoming_df = clean_snapshot(bronze_df)
    incoming_count = incoming_df.count()
    print(f"[silver.maintenance_schedules] After cleaning: {incoming_count} rows")

    if incoming_count == 0:
        print("[silver.maintenance_schedules] ⚠ All rows filtered out. Skipping.")
        return

    # ── Step 2: MERGE or Bootstrap ──────────────────────────────────
    if silver_table_exists(spark):
        # ──────────────────────────────────────────────────────
        # SCD TYPE 1 MERGE + SOFT-DELETE
        # ──────────────────────────────────────────────────────
        # Match key: (vin, service_date) — uniquely identifies a
        # maintenance event. A vehicle can only be serviced once per day.
        #
        # Case 1: Same (vin, service_date) exists but service_type changed
        #   → UPDATE. This covers rescheduling (e.g., "Tire Rotation"
        #     changed to "Full Inspection"). Also re-activates soft-deleted.
        #
        # Case 2: Same data → no-op (natural skip in MERGE).
        #
        # Case 3: New maintenance scheduled → INSERT.
        #
        # Case 4: A service date was REMOVED from the source file
        #   (cancelled maintenance). We SOFT-DELETE (is_active = FALSE)
        #   so the historical record remains queryable for past audits.
        #   If the fuel audit for April 2026 relied on knowing that a
        #   vehicle was in maintenance on May 10, we need that record
        #   to still exist even after the 2027 load removes it.
        print("[silver.maintenance_schedules] Silver table exists → Delta MERGE (SCD1 + soft-delete)")

        silver_table = DeltaTable.forPath(spark, SILVER_PATH)

        (
            silver_table.alias("existing")
            .merge(
                incoming_df.alias("incoming"),
                # Composite match key: vehicle + date uniquely identifies a service event
                "existing.vin = incoming.vin AND existing.service_date = incoming.service_date"
            )
            # Case 1: Service type changed or was soft-deleted → update
            .whenMatchedUpdate(
                condition="""
                    existing.service_type != incoming.service_type
                    OR existing.is_active = FALSE
                """,
                set={
                    "service_type":          "incoming.service_type",
                    "is_active":             "incoming.is_active",
                    "_silver_processed_at":   lit(current_timestamp()),
                }
            )
            # Case 3: New service event → insert
            .whenNotMatchedInsert(
                values={
                    "vin":                   "incoming.vin",
                    "service_date":          "incoming.service_date",
                    "service_type":          "incoming.service_type",
                    "is_active":             "incoming.is_active",
                    "_silver_processed_at":   lit(current_timestamp()),
                }
            )
            # Case 4: Service event cancelled → soft-delete
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
        print(f"[silver.maintenance_schedules] ✓ MERGE complete.")
        print(f"  Active schedules:  {active_count}")
        print(f"  Cancelled (soft-deleted): {inactive_count}")
        print(f"  Total Silver rows: {active_count + inactive_count}")

    else:
        # ──────────────────────────────────────────────────────
        # FIRST RUN (Bootstrap)
        # ──────────────────────────────────────────────────────
        print("[silver.maintenance_schedules] Silver table does NOT exist → Bootstrap write")

        incoming_df = incoming_df.withColumn("_silver_processed_at", current_timestamp())

        (
            incoming_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(SILVER_PATH)
        )

        print(f"[silver.maintenance_schedules] ✓ Bootstrap: wrote {incoming_count} rows → {SILVER_PATH}")

    # ── Post-MERGE: Move ALL Bronze partitions to processed/ ──────
    move_all_to_processed(spark, run_date)


# ──────────────────────────────────────────────
# Entry Point
# ──────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Silver transformation for maintenance_schedules (SCD1 + soft-delete)"
    )
    parser.add_argument(
        "--run-date",
        default=str(date.today()),
        help="Logical execution date (YYYY-MM-DD)",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("OmniRoute_transform_maintenance_schedules")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    try:
        run(spark, args.run_date)
    finally:
        spark.stop()
