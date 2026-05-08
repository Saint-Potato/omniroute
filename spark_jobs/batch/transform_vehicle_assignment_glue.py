"""
AWS Glue Job — Silver Transformation: Vehicle Assignment (dim_vehicle_assignment_scd2)
======================================================================================
SCD Type 2 (History/Bridge) via Delta Lake MERGE.

ER Table: dim_vehicle_assignment_scd2
Reads ONLY today's partition from Bronze (partition pruning on load_date),
cleans it, derives status, and MERGEs into the Silver Delta table.

Null Handling (Silver enforcement):
  - vin NULL/empty            → QUARANTINE ROW (FK)
  - driver_id NULL/empty      → QUARANTINE ROW (FK)
  - start_timestamp NULL      → QUARANTINE ROW (can't derive start_date)
  - daily_rate NULL/≤0        → QUARANTINE ROW
  - driver SUSPENDED          → QUARANTINE ROW (pre-merge) / ARCHIVE (post-merge)
  - region NULL/empty         → Left as NULL

Quarantine:
  - Rejected rows appended to bronze quarantine/{table_name}/ as Parquet
  - Bronze metadata (load_date, batch_id, etc.) preserved on quarantined rows
  - batch_id used for idempotency — re-runs skip if batch already quarantined

Bronze data is NOT moved — stays in ingested/ for auditability.

MERGE Logic:
  Match key: (vin, start_date)
  WHEN MATCHED AND data changed → UPDATE
  WHEN NOT MATCHED              → INSERT

Maintenance / Temporary Absence Handling:
  If a VIN is absent from the vehicle registry OR has a maintenance schedule on
  run_date, but its assignment end_date is in the future, the assignment is NOT
  permanently archived. Instead:
    1. The current record is closed with end_date = run_date.
    2. A future assignment is created with start_date = run_date + 1 day and the
       original scheduled end_date, so the assignment auto-resumes the next day.

Glue Job Parameters:
  --run_date              : Partition date (YYYY-MM-DD)
  --bronze_ingested_path  : Base S3 path for Bronze ingested data
  --silver_output_path    : S3 path for Silver Delta table output
  --silver_vehicle_path   : S3 path to Silver vehicle registry (for active VIN filter)
  --silver_maintenance_path : S3 path to Silver maintenance Delta table
  --pg_host               : PostgreSQL host (for driver safety status lookup)
  --pg_port               : PostgreSQL port (default 5432)
  --pg_database           : PostgreSQL database name
  --pg_user               : PostgreSQL username
  --pg_password           : PostgreSQL password
"""

import sys
from datetime import date, timedelta

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, trim, upper, row_number, from_unixtime, to_date,
    current_timestamp, when, lit, sha2, concat_ws, to_timestamp,
    date_add, concat,
)
from pyspark.sql.types import LongType, FloatType, DecimalType
from delta.tables import DeltaTable


# ──────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────
TABLE_NAME = "vehicle_assignment"


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def normalize_path(path):
    """Ensure S3 path ends with a trailing slash."""
    return path if path.endswith("/") else path + "/"


def derive_quarantine_path(bronze_ingested_path):
    """Derive quarantine base from bronze ingested path.
    e.g. s3://bucket/prefix/ingested/ → s3://bucket/prefix/quarantine/
    """
    return bronze_ingested_path.replace("/ingested/", "/quarantine/")


def derive_future_path(bronze_ingested_path):
    """Derive future_vehicle_assignments base from bronze ingested path.
    e.g. s3://bucket/prefix/ingested/ → s3://bucket/prefix/ingested/future_vehicle_assignments/
    """
    return bronze_ingested_path + "future_vehicle_assignments/"


def silver_table_exists(spark, path):
    """Check whether the Silver Delta table already exists."""
    try:
        DeltaTable.forPath(spark, path)
        return True
    except Exception:
        return False


def load_suspended_drivers(spark, jdbc_url, connection_props):
    """Read SUSPENDED drivers from PostgreSQL report.driver_safety_status.

    Returns a DataFrame of suspended driver_ids, or None if the table
    is empty, unreachable, or has no suspended drivers.
    Drivers NOT in this table are treated as ACTIVE (no filtering).
    """
    try:
        df = (
            spark.read.jdbc(
                url=jdbc_url,
                table="report.driver_safety_status",
                properties=connection_props,
            )
            .filter(col("status") == "SUSPENDED")
            .select(col("driver_id").alias("suspended_driver_id"))
        )
        count = df.count()
        print(f"[dim_vehicle_assignment_scd2] Suspended drivers in PostgreSQL: {count}")
        return df if count > 0 else None
    except Exception as e:
        print(f"[dim_vehicle_assignment_scd2] ⚠ Could not read driver_safety_status from PostgreSQL: {e}")
        print("[dim_vehicle_assignment_scd2] ⚠ Treating all drivers as ACTIVE (no suspended driver filtering).")
        return None


# ──────────────────────────────────────────────────────────────
# Quarantine Helpers
# ──────────────────────────────────────────────────────────────
def _tag_rejected(df, reason):
    """Tag rejected rows with reason. Preserves all columns including
    bronze metadata (load_date, batch_id, ingestion_timestamp, source_file_name).
    """
    return (
        df.select([col(c).cast("string").alias(c) for c in df.columns])
        .withColumn("rejection_reason", lit(reason))
        .withColumn("rejected_at", current_timestamp())
    )


def write_quarantine(spark, quarantine_dfs, quarantine_base, table_name):
    """Union all quarantine DFs and append to quarantine path.
    Uses batch_id from bronze metadata for idempotency.
    """
    if not quarantine_dfs:
        print(f"[{table_name}] ✓ No rows quarantined")
        return

    combined = quarantine_dfs[0]
    for qdf in quarantine_dfs[1:]:
        combined = combined.unionByName(qdf, allowMissingColumns=True)

    count = combined.count()
    if count == 0:
        print(f"[{table_name}] ✓ No rows quarantined")
        return

    output_path = f"{quarantine_base}{table_name}"

    # ── Idempotency: skip if batch_id already in quarantine ──
    if "batch_id" in combined.columns:
        batch_ids = [
            r["batch_id"] for r in
            combined.select("batch_id").distinct().collect()
            if r["batch_id"] is not None
        ]
        if batch_ids:
            try:
                existing = spark.read.parquet(output_path)
                already = existing.filter(
                    col("batch_id").isin(batch_ids)
                ).select("batch_id").distinct().count()
                if already > 0:
                    print(f"[{table_name}] ⚠ batch_id already in quarantine — skipping (idempotent)")
                    return
            except Exception:
                pass  # Quarantine doesn't exist yet

    combined.write.mode("append").parquet(output_path)
    print(f"[{table_name}] ✗ Quarantined {count} rejected rows → {output_path}")


# ──────────────────────────────────────────────────────────────
# Data Cleaning
# ──────────────────────────────────────────────────────────────
def clean_incoming_batch(bronze_df, run_date, suspended_df=None):
    """
    Clean a batch of Bronze vehicle assignment records.
    Enforces null handling, dedup, status derivation, suspended driver
    check, and ER schema.

    Args:
        bronze_df: Raw Bronze DataFrame
        run_date: Date string (YYYY-MM-DD)
        suspended_df: Optional DataFrame of suspended driver_ids from PostgreSQL.
                      If None, suspended driver check is skipped.

    Returns:
        (clean_df, quarantine_dfs): Tuple of the cleaned DataFrame and a list
        of DataFrames containing rejected rows tagged with rejection reasons.
    """
    quarantine_dfs = []
    total_before = bronze_df.count()

    # ── Quarantine NULL vin or driver_id (both required FKs) ──
    rejected_keys = bronze_df.filter(
        col("vin").isNull() | (col("vin") == "")
        | col("driver_id").isNull() | (col("driver_id") == "")
    )
    df = bronze_df.filter(
        col("vin").isNotNull() & (col("vin") != "")
        & col("driver_id").isNotNull() & (col("driver_id") != "")
    )
    dropped_keys = rejected_keys.count()
    if dropped_keys > 0:
        print(f"[dim_vehicle_assignment_scd2] Rejected {dropped_keys} rows with NULL vin/driver_id")
        quarantine_dfs.append(_tag_rejected(rejected_keys, "NULL_OR_EMPTY_VIN_OR_DRIVER_ID"))

    # ── Normalize VIN + driver_id → UPPERCASE ──
    df = df.withColumn("vin", trim(upper(col("vin"))))
    df = df.withColumn("driver_id", trim(upper(col("driver_id"))))

    # ── Convert Unix timestamps → Timestamp (preserves time) + Date (for keys) ──
    df = (
        df
        .withColumn(
            "start_datetime",
            from_unixtime(col("start_timestamp").cast(LongType())).cast("timestamp")
        )
        .withColumn(
            "start_date",
            to_date(col("start_datetime"))
        )
        .withColumn(
            "end_datetime",
            when(
                col("end_timestamp").isNotNull(),
                from_unixtime(col("end_timestamp").cast(LongType())).cast("timestamp")
            ).otherwise(lit(None).cast("timestamp"))
        )
        .withColumn(
            "end_date",
            to_date(col("end_datetime"))
        )
    )

    # ── Quarantine rows where start_timestamp couldn't be parsed ──
    rejected_start = df.filter(col("start_date").isNull())
    df = df.filter(col("start_date").isNotNull())
    dropped_start = rejected_start.count()
    if dropped_start > 0:
        print(f"[dim_vehicle_assignment_scd2] Rejected {dropped_start} rows with NULL start_date")
        quarantine_dfs.append(_tag_rejected(rejected_start, "NULL_OR_INVALID_START_TIMESTAMP"))

    # ── Quarantine NULL/invalid daily_rate ──
    rejected_rate = df.filter(
        col("daily_rate").cast(FloatType()).isNull()
        | (col("daily_rate").cast(FloatType()) <= 0)
    )
    df = df.filter(
        col("daily_rate").cast(FloatType()).isNotNull()
        & (col("daily_rate").cast(FloatType()) > 0)
    )
    dropped_rate = rejected_rate.count()
    if dropped_rate > 0:
        print(f"[dim_vehicle_assignment_scd2] Rejected {dropped_rate} rows with NULL/invalid daily_rate")
        quarantine_dfs.append(_tag_rejected(rejected_rate, "NULL_OR_INVALID_DAILY_RATE"))

    # ── Quarantine assignments where driver is SUSPENDED in PostgreSQL ──
    if suspended_df is not None:
        rejected_suspended = df.join(
            suspended_df,
            trim(upper(col("driver_id"))) == col("suspended_driver_id"),
            "inner"
        ).drop("suspended_driver_id")
        df = df.join(
            suspended_df,
            trim(upper(col("driver_id"))) == col("suspended_driver_id"),
            "left_anti"
        )
        dropped_suspended = rejected_suspended.count()
        if dropped_suspended > 0:
            print(f"[dim_vehicle_assignment_scd2] Rejected {dropped_suspended} rows — driver SUSPENDED")
            quarantine_dfs.append(_tag_rejected(rejected_suspended, "DRIVER_SUSPENDED"))

    # ── Quarantine rows where start_date > end_date ──
    rejected_logic = df.filter(col("end_date").isNotNull() & (col("start_date") > col("end_date")))
    df = df.filter(col("end_date").isNull() | (col("start_date") <= col("end_date")))
    dropped_logic = rejected_logic.count()
    if dropped_logic > 0:
        print(f"[dim_vehicle_assignment_scd2] Rejected {dropped_logic} rows with start_date > end_date")
        quarantine_dfs.append(_tag_rejected(rejected_logic, "START_DATE_AFTER_END_DATE"))

    # ── region: leave NULL as-is, TRIM + UPPER non-null values ──
    df = df.withColumn(
        "region",
        when(
            col("region").isNull() | (trim(col("region")) == ""),
            lit(None).cast("string")
        ).otherwise(trim(upper(col("region"))))
    )

    # ── Dedup WITHIN batch: same VIN + same start_date → keep highest daily_rate ──
    dedup_window = Window.partitionBy("vin", "start_date").orderBy(col("daily_rate").desc())
    df = (
        df.withColumn("_rn", row_number().over(dedup_window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    # ── Derive status and is_current using run_date ──
    df = (
        df
        .withColumn(
            "is_current",
            col("end_date").isNull() | (col("end_date") >= lit(run_date))
        )
        .withColumn(
            "status",
            when(col("is_current"), lit("IN-TRANSIT")).otherwise(lit("ARCHIVED"))
        )
    )

    # ── Build final Silver schema (no audit_run_id) ──
    df = df.select(
        sha2(concat_ws("|", col("vin"), col("start_date").cast("string")), 256).alias("assignment_sk"),
        sha2(col("vin"), 256).alias("vehicle_sk"),
        sha2(col("driver_id"), 256).alias("driver_sk"),
        col("vin"),
        col("driver_id"),
        col("region"),
        col("daily_rate").cast(DecimalType(10, 2)).alias("daily_rate"),
        col("start_datetime"),
        col("start_date"),
        col("end_datetime"),
        col("end_date"),
        col("is_current"),
        col("status"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at"),
    )

    return df, quarantine_dfs


# ──────────────────────────────────────────────────────────────
# Core Transformation Logic
# ──────────────────────────────────────────────────────────────
def run(spark, run_date, bronze_base, silver_path, silver_vehicle_path,
        silver_maintenance_path=None, jdbc_url=None, connection_props=None):
    """
    INCREMENTAL Silver transformation for vehicle_assignment.
    Reads only today's Bronze partition. Bronze data stays in place.
    Only VINs active in the Silver Vehicle Registry are kept.
    Drivers SUSPENDED in PostgreSQL driver_safety_status are quarantined/archived.
    """
    bronze_partition_path = f"{bronze_base}{TABLE_NAME}/load_date={run_date}"
    print(f"[dim_vehicle_assignment_scd2] run_date={run_date}")
    print(f"[dim_vehicle_assignment_scd2] Reading: {bronze_partition_path}")

    try:
        bronze_df = spark.read.parquet(bronze_partition_path)
    except Exception as e:
        print(f"[dim_vehicle_assignment_scd2] ⚠ No Bronze partition for {run_date}: {e}")
        bronze_df = None

    # Load existing future assignments
    future_path = derive_future_path(bronze_base)
    try:
        future_df = spark.read.parquet(future_path)
        future_df = future_df.localCheckpoint(eager=True) # Break lineage and fully materialize to avoid self-overwrite bugs
        future_df_count = future_df.count()
        print(f"[dim_vehicle_assignment_scd2] Loaded existing future assignments: {future_df_count} rows")
    except Exception:
        future_df = None

    if bronze_df is None and future_df is None:
        print("[dim_vehicle_assignment_scd2] ⚠ Empty partition and no future assignments. Skipping.")
        return

    if bronze_df is not None and future_df is not None:
        combined_df = bronze_df.unionByName(future_df, allowMissingColumns=True)
    elif bronze_df is not None:
        combined_df = bronze_df
    else:
        combined_df = future_df

    total = combined_df.count()
    print(f"[dim_vehicle_assignment_scd2] Total rows (incoming + past future): {total}")
    if total == 0:
        print("[dim_vehicle_assignment_scd2] ⚠ Empty incoming data. Skipping.")
        return

    # Extract start_date to separate future vs current records
    combined_df = combined_df.withColumn(
        "tmp_start_date",
        to_date(from_unixtime(col("start_timestamp").cast(LongType())).cast("timestamp"))
    )

    future_bronze_df = combined_df.filter(col("tmp_start_date") > lit(run_date)).drop("tmp_start_date")
    past_bronze_df = combined_df.filter(col("tmp_start_date") < lit(run_date)).drop("tmp_start_date")
    current_bronze_df = combined_df.filter(
        (col("tmp_start_date") == lit(run_date)) | col("tmp_start_date").isNull()
    ).drop("tmp_start_date")

    # Overwrite future assignments
    future_count = future_bronze_df.count()
    if future_count > 0:
        future_bronze_df.write.mode("overwrite").parquet(future_path)
        print(f"[dim_vehicle_assignment_scd2] Saved {future_count} future assignments to {future_path}")
    else:
        # Overwrite with empty DF if there were future assignments previously
        if future_df is not None:
            empty_df = spark.createDataFrame([], future_bronze_df.schema)
            empty_df.write.mode("overwrite").parquet(future_path)
        print(f"[dim_vehicle_assignment_scd2] Saved 0 future assignments to {future_path}")

    current_count = current_bronze_df.count()
    print(f"[dim_vehicle_assignment_scd2] Records to process today: {current_count}")
    
    quarantine_dfs = []
    
    past_count = past_bronze_df.count()
    if past_count > 0:
        print(f"[dim_vehicle_assignment_scd2] Found {past_count} past assignments. Quarantining...")
        past_quarantine_df = _tag_rejected(past_bronze_df, "PAST_ASSIGNMENT_DATE")
        quarantine_dfs.append(past_quarantine_df)

    # ── Load suspended drivers from PostgreSQL (if available) ──
    suspended_df = None
    if jdbc_url and connection_props:
        suspended_df = load_suspended_drivers(spark, jdbc_url, connection_props)

    incoming_df, batch_quarantine_dfs = clean_incoming_batch(current_bronze_df, run_date, suspended_df)
    quarantine_dfs.extend(batch_quarantine_dfs)

    # ── Write quarantined rows to bronze quarantine ──
    quarantine_base = derive_quarantine_path(bronze_base)
    write_quarantine(spark, quarantine_dfs, quarantine_base, TABLE_NAME)

    count = incoming_df.count()
    print(f"[dim_vehicle_assignment_scd2] After cleaning: {count} rows")
    if count == 0 and past_count == 0:
        print("[dim_vehicle_assignment_scd2] ⚠ All rows filtered out and no past records to quarantine. Skipping.")
        return
    elif count == 0:
        print("[dim_vehicle_assignment_scd2] ⚠ All rows filtered out. Skipping merge.")
        return


    # ── NO pre-MERGE VIN filter — we MERGE all incoming data first, ──
    # ── then archive assignments for inactive VINs post-MERGE.      ──

    if silver_table_exists(spark, silver_path):
        # ── SCD TYPE 2: TWO-PASS MERGE ──
        # SCD2 requires TWO operations because we need to both:
        #   1. CLOSE the old active record (set end_date, is_current=False)
        #   2. INSERT the new active record
        # A single MERGE cannot do both for the same incoming row.
        print("[dim_vehicle_assignment_scd2] Silver exists → Two-Pass SCD2 MERGE")

        # Enable schema auto-merge for any new columns
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        # ─── PASS 1: Close old active records ───
        # Match: same VIN + currently active (is_current=True)
        # If incoming has a DIFFERENT start_date → this is a new assignment
        # → close the old one by setting end_date = incoming.start_date
        print("[dim_vehicle_assignment_scd2] Pass 1: Closing old active records...")
        silver_table_p1 = DeltaTable.forPath(spark, silver_path)
        (
            silver_table_p1.alias("existing")
            .merge(
                incoming_df.alias("incoming"),
                "existing.vin = incoming.vin AND existing.is_current = TRUE"
            )
            .whenMatchedUpdate(
                # Only close if this is a genuinely NEW assignment (different start_date)
                condition="existing.start_date != incoming.start_date",
                set={
                    "end_date":       "incoming.start_date",
                    "end_datetime":   "incoming.start_datetime",
                    "is_current":     lit(False),
                    "status":         lit("ARCHIVED"),
                    "updated_at":     current_timestamp(),
                }
            )
            .execute()
        )
        closed_count = spark.read.format("delta").load(silver_path).filter(
            (col("is_current") == False) & (col("status") == "ARCHIVED")  # noqa: E712
        ).count()
        print(f"[dim_vehicle_assignment_scd2] Pass 1 done. Total ARCHIVED records: {closed_count}")

        # ─── PASS 1.5: Enforce 1 Driver = 1 Vehicle ───
        # If the incoming data assigns a driver to a NEW vehicle, we must close
        # any OTHER vehicles that driver is currently assigned to.
        print("[dim_vehicle_assignment_scd2] Pass 1.5: Closing old vehicles for reassigned drivers...")
        silver_table_p1_5 = DeltaTable.forPath(spark, silver_path)
        (
            silver_table_p1_5.alias("existing")
            .merge(
                incoming_df.alias("incoming"),
                "existing.driver_id = incoming.driver_id AND existing.is_current = TRUE"
            )
            .whenMatchedUpdate(
                # Close the driver's old vehicle assignment
                condition="existing.vin != incoming.vin",
                set={
                    "end_date":       "incoming.start_date",
                    "end_datetime":   "incoming.start_datetime",
                    "is_current":     lit(False),
                    "status":         lit("ARCHIVED"),
                    "updated_at":     current_timestamp(),
                }
            )
            .execute()
        )

        # ─── PASS 2: Insert new records / Update existing records ───
        # Match: same VIN + same start_date (unique assignment key)
        # - If matched AND data changed → UPDATE (rate correction, driver swap)
        # - If not matched → INSERT (new assignment)
        print("[dim_vehicle_assignment_scd2] Pass 2: Inserting new / updating existing...")
        silver_table_p2 = DeltaTable.forPath(spark, silver_path)
        (
            silver_table_p2.alias("existing")
            .merge(
                incoming_df.alias("incoming"),
                "existing.vin = incoming.vin AND existing.start_date = incoming.start_date"
            )
            .whenMatchedUpdate(
                condition="""
                    incoming.daily_rate != existing.daily_rate
                    OR incoming.driver_id != existing.driver_id
                    OR (incoming.end_date IS NOT NULL AND existing.end_date IS NULL)
                    OR (incoming.end_date IS NULL AND existing.end_date IS NOT NULL)
                    OR (incoming.end_date != existing.end_date)
                    OR (incoming.status != existing.status)
                """,
                set={
                    "driver_id":      "incoming.driver_id",
                    "driver_sk":      "incoming.driver_sk",
                    "end_datetime":   "incoming.end_datetime",
                    "end_date":       "incoming.end_date",
                    "daily_rate":     "incoming.daily_rate",
                    "region":         "incoming.region",
                    "status":         "incoming.status",
                    "is_current":     "incoming.is_current",
                    "updated_at":     current_timestamp(),
                }
            )
            .whenNotMatchedInsert(values={
                "assignment_sk":  "incoming.assignment_sk",
                "vehicle_sk":     "incoming.vehicle_sk",
                "driver_sk":      "incoming.driver_sk",
                "vin":            "incoming.vin",
                "driver_id":      "incoming.driver_id",
                "region":         "incoming.region",
                "daily_rate":     "incoming.daily_rate",
                "start_datetime": "incoming.start_datetime",
                "start_date":     "incoming.start_date",
                "end_datetime":   "incoming.end_datetime",
                "end_date":       "incoming.end_date",
                "is_current":     "incoming.is_current",
                "status":         "incoming.status",
                "created_at":     "incoming.created_at",
                "updated_at":     "incoming.updated_at",
            })
            .execute()
        )

        final_df = spark.read.format("delta").load(silver_path)
        active_before = final_df.filter(col("is_current") == True).count()   # noqa: E712
        print(f"[dim_vehicle_assignment_scd2] ✓ MERGE complete. IN-TRANSIT before registry check: {active_before}")

        # ── POST-MERGE: Archive assignments where end_date < run_date ──
        try:
            expired_assignments = final_df.filter(
                (col("is_current") == True) & 
                col("end_date").isNotNull() & 
                (col("end_date") < lit(run_date))
            )
            expired_count = expired_assignments.count()
            if expired_count > 0:
                silver_table_post_exp = DeltaTable.forPath(spark, silver_path)
                silver_table_post_exp.update(
                    condition=(
                        (col("is_current") == True) &
                        col("end_date").isNotNull() &
                        (col("end_date") < lit(run_date))
                    ),
                    set={
                        "is_current": lit(False),
                        "status":     lit("ARCHIVED"),
                        "updated_at": current_timestamp(),
                    }
                )
                print(f"[dim_vehicle_assignment_scd2] ✓ Archived {expired_count} assignments — end_date passed")
                
                # Refresh final_df after update
                final_df = spark.read.format("delta").load(silver_path)
        except Exception as e:
            print(f"[dim_vehicle_assignment_scd2] ⚠ Could not enforce expired end_date check: {e}")

        # ── POST-MERGE: Handle assignments whose VIN is no longer active ──
        # Instead of permanently archiving all absent VINs, check if the
        # assignment has a future end_date. If so, close the record and send
        # a future assignment so it resumes when the vehicle returns.
        try:
            unique_vins_before = final_df.filter(col("is_current") == True).select("vin").distinct().count()  # noqa: E712
            print(f"[dim_vehicle_assignment_scd2] Unique VINs with is_current=True (before registry check): {unique_vins_before}")

            active_vins_df = (
                spark.read.format("delta").load(silver_vehicle_path)
                .filter(col("is_active") == True)  # noqa: E712
                .select(col("vin").alias("reg_vin"))
            )
            active_vin_count = active_vins_df.count()
            print(f"[dim_vehicle_assignment_scd2] Active VINs in vehicle registry: {active_vin_count}")

            # Find VINs in Silver assignment that are is_current=True but NOT in active registry
            inactive_assignments = (
                final_df.filter(col("is_current") == True)  # noqa: E712
                .join(active_vins_df, col("vin") == col("reg_vin"), "left_anti")
            )
            inactive_count = inactive_assignments.count()

            if inactive_count > 0:
                inactive_unique_vins = inactive_assignments.select("vin").distinct().count()
                print(f"[dim_vehicle_assignment_scd2] VINs absent from active registry: {inactive_unique_vins} unique VINs, {inactive_count} assignment records")

                # Split: assignments with future end_date OR open-ended (NULL) → re-queue, others → permanent archive
                next_day = str((date.fromisoformat(run_date) + timedelta(days=1)))

                requeue_df = inactive_assignments.filter(
                    col("end_date").isNull() | (col("end_date") > lit(run_date))
                )
                permanent_df = inactive_assignments.filter(
                    col("end_date").isNotNull() & (col("end_date") <= lit(run_date))
                )

                requeue_count = requeue_df.count()
                permanent_count = permanent_df.count()

                # ── Re-queue: create future assignments for VINs with future end_date ──
                if requeue_count > 0:
                    requeue_vins = [row["vin"] for row in requeue_df.select("vin").distinct().collect()]
                    print(f"[dim_vehicle_assignment_scd2] Re-queuing {requeue_count} assignments to future (VIN temporarily absent, end_date in future)")

                    # Build future assignment rows from the current Silver records
                    future_rows = (
                        requeue_df.select(
                            col("vin"),
                            col("driver_id"),
                            col("region"),
                            col("daily_rate"),
                            col("end_date").alias("original_end_date"),
                            col("end_datetime").alias("original_end_datetime"),
                        )
                    )

                    # Convert Silver columns back to bronze-like format for future_vehicle_assignments
                    # The future path stores raw-ish records that get re-processed on the next run
                    future_records = (
                        future_rows
                        .withColumn("start_timestamp",
                            col("vin").cast("string"))  # placeholder, will be replaced below
                        .withColumn("end_timestamp",
                            col("vin").cast("string"))  # placeholder, will be replaced below
                    )

                    # We need unix timestamps for start/end to match bronze schema.
                    # start_timestamp = next_day at midnight, end_timestamp = original_end_datetime
                    from pyspark.sql.functions import unix_timestamp as _unix_ts
                    future_records = (
                        future_rows
                        .withColumn("start_timestamp",
                            _unix_ts(lit(next_day + " 00:00:00"), "yyyy-MM-dd HH:mm:ss").cast(LongType()))
                        .withColumn("end_timestamp",
                            when(col("original_end_datetime").isNotNull(),
                                 _unix_ts(col("original_end_datetime").cast("string"), "yyyy-MM-dd HH:mm:ss").cast(LongType())
                            ).otherwise(
                                _unix_ts(concat(col("original_end_date").cast("string"), lit(" 23:59:59")), "yyyy-MM-dd HH:mm:ss").cast(LongType())
                            ))
                        .withColumn("status", lit("PENDING_RESUME"))
                        .drop("original_end_date", "original_end_datetime")
                    )

                    # Append to future_vehicle_assignments
                    future_path = derive_future_path(bronze_base)
                    try:
                        existing_future = spark.read.parquet(future_path)
                        existing_future = existing_future.localCheckpoint(eager=True)
                        combined_future = existing_future.unionByName(future_records, allowMissingColumns=True)
                    except Exception:
                        combined_future = future_records
                    combined_future.write.mode("overwrite").parquet(future_path)
                    print(f"[dim_vehicle_assignment_scd2] ✓ Saved {requeue_count} future assignments (VIN absent, end_date in future) → {future_path}")

                    # Close these records in Silver (end_date = run_date)
                    silver_table_requeue = DeltaTable.forPath(spark, silver_path)
                    silver_table_requeue.update(
                        condition=(
                            (col("is_current") == True) &  # noqa: E712
                            col("vin").isin(requeue_vins)
                        ),
                        set={
                            "end_date":     lit(run_date).cast("date"),
                            "end_datetime": current_timestamp(),
                            "is_current":   lit(False),
                            "status":       lit("ARCHIVED"),
                            "updated_at":   current_timestamp(),
                        }
                    )
                    print(f"[dim_vehicle_assignment_scd2] ✓ Closed {requeue_count} Silver records with ARCHIVED status")

                # ── Permanent archive: VINs with no future end_date ──
                if permanent_count > 0:
                    permanent_vins = [row["vin"] for row in permanent_df.select("vin").distinct().collect()]
                    silver_table_post = DeltaTable.forPath(spark, silver_path)
                    silver_table_post.update(
                        condition=(
                            (col("is_current") == True) &  # noqa: E712
                            col("vin").isin(permanent_vins)
                        ),
                        set={
                            "is_current": lit(False),
                            "status":     lit("ARCHIVED"),
                            "updated_at": current_timestamp(),
                        }
                    )
                    print(f"[dim_vehicle_assignment_scd2] ✓ Archived {permanent_count} assignments — VIN no longer active (no future end_date)")
            else:
                print("[dim_vehicle_assignment_scd2] ✓ All current assignments have active VINs")

        except Exception as e:
            print(f"[dim_vehicle_assignment_scd2] ⚠ Could not enforce active VIN check: {e}. Skipping post-MERGE archive.")

        # ── POST-MERGE: Handle assignments whose VIN is in maintenance today ──
        # Same logic as absent VINs: if end_date is in the future, pause + re-queue.
        if silver_maintenance_path:
            try:
                maintenance_df = (
                    spark.read.format("delta").load(silver_maintenance_path)
                    .filter(col("service_date") == lit(run_date))
                    .select(col("vin").alias("maint_vin"))
                    .distinct()
                )
                maint_vin_count = maintenance_df.count()
                print(f"[dim_vehicle_assignment_scd2] VINs in maintenance on {run_date}: {maint_vin_count}")

                if maint_vin_count > 0:
                    # Refresh Silver after prior updates
                    final_df_maint = spark.read.format("delta").load(silver_path)

                    # Find current assignments for vehicles in maintenance today
                    maint_assignments = (
                        final_df_maint.filter(col("is_current") == True)  # noqa: E712
                        .join(maintenance_df, col("vin") == col("maint_vin"), "inner")
                        .drop("maint_vin")
                    )
                    maint_assignment_count = maint_assignments.count()

                    if maint_assignment_count > 0:
                        next_day = str((date.fromisoformat(run_date) + timedelta(days=1)))

                        # Split: future end_date OR open-ended (NULL) → pause + re-queue, else → archive
                        maint_requeue = maint_assignments.filter(
                            col("end_date").isNull() | (col("end_date") > lit(run_date))
                        )
                        maint_permanent = maint_assignments.filter(
                            col("end_date").isNotNull() & (col("end_date") <= lit(run_date))
                        )

                        maint_requeue_count = maint_requeue.count()
                        maint_permanent_count = maint_permanent.count()

                        if maint_requeue_count > 0:
                            maint_requeue_vins = [row["vin"] for row in maint_requeue.select("vin").distinct().collect()]
                            print(f"[dim_vehicle_assignment_scd2] Re-queuing {maint_requeue_count} assignments to future (VIN in maintenance, end_date in future)")

                            # Build future assignment rows
                            from pyspark.sql.functions import unix_timestamp as _unix_ts
                            future_maint_rows = (
                                maint_requeue.select(
                                    col("vin"),
                                    col("driver_id"),
                                    col("region"),
                                    col("daily_rate"),
                                    col("end_date").alias("original_end_date"),
                                    col("end_datetime").alias("original_end_datetime"),
                                )
                                .withColumn("start_timestamp",
                                    _unix_ts(lit(next_day + " 00:00:00"), "yyyy-MM-dd HH:mm:ss").cast(LongType()))
                                .withColumn("end_timestamp",
                                    when(col("original_end_datetime").isNotNull(),
                                         _unix_ts(col("original_end_datetime").cast("string"), "yyyy-MM-dd HH:mm:ss").cast(LongType())
                                    ).otherwise(
                                        _unix_ts(concat(col("original_end_date").cast("string"), lit(" 23:59:59")), "yyyy-MM-dd HH:mm:ss").cast(LongType())
                                    ))
                                .withColumn("status", lit("PENDING_RESUME"))
                                .drop("original_end_date", "original_end_datetime")
                            )

                            # Append to future_vehicle_assignments
                            future_path = derive_future_path(bronze_base)
                            try:
                                existing_future = spark.read.parquet(future_path)
                                existing_future = existing_future.localCheckpoint(eager=True)
                                combined_future = existing_future.unionByName(future_maint_rows, allowMissingColumns=True)
                            except Exception:
                                combined_future = future_maint_rows
                            combined_future.write.mode("overwrite").parquet(future_path)
                            print(f"[dim_vehicle_assignment_scd2] ✓ Saved {maint_requeue_count} maintenance future assignments → {future_path}")

                            # Close these records in Silver
                            silver_table_maint = DeltaTable.forPath(spark, silver_path)
                            silver_table_maint.update(
                                condition=(
                                    (col("is_current") == True) &  # noqa: E712
                                    col("vin").isin(maint_requeue_vins)
                                ),
                                set={
                                    "end_date":     lit(run_date).cast("date"),
                                    "end_datetime": current_timestamp(),
                                    "is_current":   lit(False),
                                    "status":       lit("ARCHIVED"),
                                    "updated_at":   current_timestamp(),
                                }
                            )
                            print(f"[dim_vehicle_assignment_scd2] ✓ Closed {maint_requeue_count} Silver records with ARCHIVED status")

                        if maint_permanent_count > 0:
                            maint_permanent_vins = [row["vin"] for row in maint_permanent.select("vin").distinct().collect()]
                            silver_table_maint_perm = DeltaTable.forPath(spark, silver_path)
                            silver_table_maint_perm.update(
                                condition=(
                                    (col("is_current") == True) &  # noqa: E712
                                    col("vin").isin(maint_permanent_vins)
                                ),
                                set={
                                    "is_current": lit(False),
                                    "status":     lit("ARCHIVED"),
                                    "updated_at": current_timestamp(),
                                }
                            )
                            print(f"[dim_vehicle_assignment_scd2] ✓ Archived {maint_permanent_count} assignments — VIN in maintenance (no future end_date)")
                    else:
                        print("[dim_vehicle_assignment_scd2] ✓ No current assignments affected by maintenance")
                else:
                    print(f"[dim_vehicle_assignment_scd2] ✓ No vehicles in maintenance on {run_date}")

            except Exception as e:
                print(f"[dim_vehicle_assignment_scd2] ⚠ Could not enforce maintenance check: {e}. Skipping.")

        # ── POST-MERGE: Archive assignments whose driver is now SUSPENDED ──
        # Catches IN-TRANSIT assignments where the driver got suspended after assignment.
        if suspended_df is not None:
            try:
                final_df_susp = spark.read.format("delta").load(silver_path)
                suspended_assignments = (
                    final_df_susp.filter(col("is_current") == True)  # noqa: E712
                    .join(suspended_df, col("driver_id") == col("suspended_driver_id"), "inner")
                    .select("vin", "start_date", "driver_id")
                )
                suspended_count = suspended_assignments.count()

                if suspended_count > 0:
                    suspended_driver_ids = [
                        row["driver_id"] for row in
                        suspended_assignments.select("driver_id").distinct().collect()
                    ]
                    print(f"[dim_vehicle_assignment_scd2] Drivers to archive (SUSPENDED): "
                          f"{len(suspended_driver_ids)} drivers, {suspended_count} assignment records")

                    silver_table_susp = DeltaTable.forPath(spark, silver_path)
                    silver_table_susp.update(
                        condition=(
                            (col("is_current") == True) &  # noqa: E712
                            col("driver_id").isin(suspended_driver_ids)
                        ),
                        set={
                            "is_current": lit(False),
                            "status":     lit("ARCHIVED"),
                            "updated_at": current_timestamp(),
                        }
                    )
                    print(f"[dim_vehicle_assignment_scd2] ✓ Archived {suspended_count} assignments — driver SUSPENDED")
                else:
                    print("[dim_vehicle_assignment_scd2] ✓ No current assignments have suspended drivers")

            except Exception as e:
                print(f"[dim_vehicle_assignment_scd2] ⚠ Could not enforce suspended driver check: {e}")

        # ── Final counts ──
        final_df2 = spark.read.format("delta").load(silver_path)
        active = final_df2.filter(col("is_current") == True).count()     # noqa: E712
        archived = final_df2.filter(col("is_current") == False).count()  # noqa: E712
        unique_active_vins = final_df2.filter(col("is_current") == True).select("vin").distinct().count()  # noqa: E712
        print(f"[dim_vehicle_assignment_scd2] Final counts:")
        print(f"  IN-TRANSIT: {active} | ARCHIVED: {archived} | Total: {active + archived}")
        print(f"  Unique VINs with active assignment: {unique_active_vins}")

        # ── VACUUM: Delete old Parquet files no longer in Delta log ──
        DeltaTable.forPath(spark, silver_path).vacuum()
        print("[dim_vehicle_assignment_scd2] ✓ VACUUM complete — old Parquet files deleted.")

    else:
        # ── FIRST RUN (Bootstrap) ──
        print("[dim_vehicle_assignment_scd2] Silver does NOT exist → Bootstrap")
        (
            incoming_df.write.format("delta")
            .mode("overwrite").option("overwriteSchema", "true")
            .save(silver_path)
        )
        print(f"[dim_vehicle_assignment_scd2] ✓ Bootstrap: {count} rows → {silver_path}")


# ──────────────────────────────────────────────────────────────
# Glue Job Entry Point
# ──────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "run_date",
    "bronze_ingested_path",
    "silver_output_path",
    "silver_vehicle_path",
    "silver_maintenance_path",
    "pg_host",
    "pg_port",
    "pg_database",
    "pg_user",
    "pg_password",
])

# ── Delta Lake requires extensions set BEFORE SparkSession creation ──
conf = SparkConf()
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

sc = SparkContext(conf=conf)
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

bronze_base = normalize_path(args["bronze_ingested_path"])
silver_path = args["silver_output_path"].rstrip("/")
silver_vehicle_path = args["silver_vehicle_path"].rstrip("/")
silver_maintenance_path = args.get("silver_maintenance_path", "").rstrip("/") or None
run_date = args.get("run_date", str(date.today()))

# ── Build JDBC URL and connection properties for PostgreSQL ──
jdbc_url = f"jdbc:postgresql://{args['pg_host']}:{args['pg_port']}/{args['pg_database']}"
connection_props = {
    "user": args["pg_user"],
    "password": args["pg_password"],
    "driver": "org.postgresql.Driver",
    "sslmode": "require",
}

print("=" * 60)
print(f"  dim_vehicle_assignment_scd2 Silver Transformation — Glue Job")
print(f"  Run Date     : {run_date}")
print(f"  Bronze Base  : {bronze_base}")
print(f"  Silver Path  : {silver_path}")
print(f"  PG Host      : {args['pg_host']}")
print("=" * 60)

try:
    run(spark, run_date, bronze_base, silver_path, silver_vehicle_path,
        silver_maintenance_path, jdbc_url, connection_props)
    print("✓ dim_vehicle_assignment_scd2 transformation completed successfully.")
except Exception as e:
    print(f"✗ dim_vehicle_assignment_scd2 transformation failed: {e}")
    raise
finally:
    job.commit()
