"""
===========================================================
OmniRoute Smart Logistics Engine — Telemetry Consumer V2
===========================================================

PySpark Structured Streaming Kafka consumer implementing the
FULL streaming pipeline required by the BRD:

  1. Raw Ingestion        → Bronze Parquet (date-partitioned)
  2. Violation Detection  → Speed > 110 km/h + Geofence (broadcast join)
  3. Safety Strike Count  → Stateful per-driver tracking
  4. Penalty Calculation  → 5% deduction per strike
  5. Suspension Toggling  → 10 strikes → SUSPENDED
  6. Gold Layer Output    → driver_safety_status (Parquet)
  7. Postgres Export       → Reporting Layer (gold.driver_safety_status)
  8. Dead Letter Queue    → Malformed events quarantined

Architecture:
  - Spark Structured Streaming with foreachBatch for micro-batch processing
  - Broadcast join with restricted_zones for geofence detection
  - Left join with vehicle_assignment for driver resolution (BRD: stream-static join)
  - Watermarking on event_timestamp to handle late-arriving data (3-day threshold)
  - Checkpointing for exactly-once processing and crash recovery
  - Strike state persisted to JSON on disk for cross-restart durability

Usage:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
        data/scripts/consumer_scripts/telemetry_consumer_v2.py
"""

import json
import os
import sys
import time
import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, lit, current_timestamp, to_timestamp, to_date,
    broadcast, when, concat_ws, date_format, year, month, dayofmonth,
    max as spark_max, udf
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    FloatType, TimestampType, BooleanType
)

# ================================================================
# PATH SETUP & CONFIG IMPORTS
# ================================================================

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

from config import (
    TELEMETRY_CONFIG,
    TELEMETRY_RAW_DIR,
    VEHICLE_ASSIGNMENT_FILE,
    RESTRICTED_ZONES_FILE,
    PROCESSED_DIR,
    POSTGRES_CONFIG,
)

# PostgreSQL support (optional — pipeline doesn't crash if unavailable)
try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    psycopg2 = None

# ================================================================
# LOGGING
# ================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("TelemetryConsumerV2_PySpark")

AUDIT_LOG_FILE = os.path.join(CURRENT_DIR, "omniroute_audit.txt")

# ================================================================
# CONSTANTS (FROM BRD)
# ================================================================

SPEED_THRESHOLD = 110            # km/h — BRD Section 3.3
PENALTY_RATE = 0.05              # 5% deduction per strike — BRD Section 3.3
SUSPENSION_STRIKE_LIMIT = 10     # 10 strikes → SUSPENDED — BRD Section 3.3
COOLDOWN_HOUR = 5                # 05:00 UTC — BRD Section 3.3
LATE_DATA_THRESHOLD = "3 days"   # Watermark window for late arrivals

# ================================================================
# KAFKA CONFIG
# ================================================================

KAFKA_TOPIC = TELEMETRY_CONFIG["KAFKA_TOPIC"]
KAFKA_SERVER = TELEMETRY_CONFIG["KAFKA_SERVER"]

# ================================================================
# OUTPUT PATHS
# ================================================================

VIOLATIONS_DIR = os.path.join(PROCESSED_DIR, "violations")
SAFETY_STATUS_DIR = os.path.join(PROCESSED_DIR, "driver_safety_status")
DLQ_DIR = os.path.join(TELEMETRY_RAW_DIR, "dlq")
STATE_FILE = os.path.join(PROCESSED_DIR, "strike_state.json")
CHECKPOINT_DIR = os.path.join(PROCESSED_DIR, "checkpoints", "telemetry_v2")

os.makedirs(VIOLATIONS_DIR, exist_ok=True)
os.makedirs(SAFETY_STATUS_DIR, exist_ok=True)
os.makedirs(DLQ_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

# ================================================================
# TELEMETRY JSON SCHEMA (matches producer output)
# ================================================================

TELEMETRY_SCHEMA = StructType([
    StructField("vin", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("speed", IntegerType(), True),
    StructField("lat", DoubleType(), True),
    StructField("long", DoubleType(), True),
    StructField("event_timestamp", StringType(), True),
])

# ================================================================
# 1. REFERENCE DATA LOADERS (as Spark DataFrames)
# ================================================================

def load_restricted_zones_df(spark: SparkSession) -> DataFrame:
    """
    Load restricted_zones.json into a Spark DataFrame, filtering
    out invalid zones (out-of-range coords, oversized bounding boxes,
    empty objects) as per BRD data quality requirements.
    """
    if not os.path.exists(RESTRICTED_ZONES_FILE):
        logger.warning(f"Restricted zones file not found: {RESTRICTED_ZONES_FILE}")
        schema = StructType([
            StructField("zone_name", StringType()),
            StructField("min_lat", DoubleType()),
            StructField("max_lat", DoubleType()),
            StructField("min_long", DoubleType()),
            StructField("max_long", DoubleType()),
        ])
        return spark.createDataFrame([], schema)

    zones_df = spark.read.json(RESTRICTED_ZONES_FILE, multiLine=True)

    # Filter invalid zones
    zones_df = zones_df.filter(
        col("min_lat").isNotNull() &
        col("max_lat").isNotNull() &
        col("min_long").isNotNull() &
        col("max_long").isNotNull() &
        (col("min_lat").between(-90, 90)) &
        (col("max_lat").between(-90, 90)) &
        (col("min_long").between(-180, 180)) &
        (col("max_long").between(-180, 180)) &
        ((col("max_lat") - col("min_lat")) <= 0.5) &
        ((col("max_long") - col("min_long")) <= 0.5)
    ).select("zone_name", "min_lat", "max_lat", "min_long", "max_long")

    logger.info(f"Loaded {zones_df.count()} valid restricted zones into Spark DataFrame")
    return zones_df


def load_assignments_df(spark: SparkSession) -> DataFrame:
    """
    Load vehicle_assignment.csv into a Spark DataFrame.
    Filters to active assignments (no end_timestamp or future end_timestamp).
    Resolves conflicts by keeping highest daily_rate per VIN (BRD requirement).
    """
    if not os.path.exists(VEHICLE_ASSIGNMENT_FILE):
        logger.warning(f"Assignment file not found: {VEHICLE_ASSIGNMENT_FILE}")
        schema = StructType([
            StructField("vin", StringType()),
            StructField("assign_driver_id", StringType()),
            StructField("daily_rate", FloatType()),
        ])
        return spark.createDataFrame([], schema)

    raw_df = spark.read.option("header", "true").csv(VEHICLE_ASSIGNMENT_FILE)
    current_ts = int(time.time())

    # Filter to active assignments: end_timestamp is blank OR in the future
    active_df = raw_df.filter(
        (col("end_timestamp").isNull()) |
        (col("end_timestamp") == "") |
        (col("end_timestamp").cast("long") > current_ts)
    )

    # Cast daily_rate to float
    active_df = active_df.withColumn("daily_rate", col("daily_rate").cast(FloatType()))

    # BRD conflict resolution: keep highest daily_rate per VIN
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc

    w = Window.partitionBy("vin").orderBy(desc("daily_rate"))
    deduped_df = (
        active_df
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
        .select(
            col("vin"),
            col("driver_id").alias("assign_driver_id"),
            col("daily_rate"),
        )
    )

    logger.info(f"Loaded {deduped_df.count()} active assignments into Spark DataFrame")
    return deduped_df


# ================================================================
# 2. STATE MANAGEMENT & COOLDOWN (BRD LOGIC)
# ================================================================

def get_effective_month(event_ts=None):
    """
    BRD Section 3.3: Cooldown happens on the 1st of the month at 05:00 UTC.
    Uses event_timestamp if provided, otherwise fallback to current time.
    """
    if event_ts:
        now = datetime.utcfromtimestamp(event_ts)
    else:
        now = datetime.utcnow()

    if now.day == 1 and now.hour < COOLDOWN_HOUR:
        return (now - timedelta(days=1)).strftime("%Y-%m")
    return now.strftime("%Y-%m")


def load_strike_state():
    """Load persisted strike state from JSON for crash recovery."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
            logger.info(f"Loaded strike state for {len(state)} drivers")
            return state
        except Exception as e:
            logger.error(f"Failed to load state file: {e}. Starting fresh.")
    return {}


def save_strike_state(state):
    """Atomic write: .tmp → os.replace() to prevent corruption."""
    tmp_file = STATE_FILE + ".tmp"
    with open(tmp_file, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp_file, STATE_FILE)


def log_audit(message):
    """Append a message to the omniroute_audit.txt file."""
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    try:
        with open(AUDIT_LOG_FILE, "a") as f:
            f.write(f"\n[{timestamp}] STREAMING: {message}")
    except Exception as e:
        logger.error(f"Audit log failed: {e}")


def check_monthly_cooldown(state, max_event_ts=None):
    """
    Apply cooldown reset if we cross into a new effective month.
    BRD: SUSPENDED drivers are excluded from cooldown (strikes persist).
    """
    effective_month = get_effective_month(max_event_ts)
    reset_count = 0

    for driver_id, info in state.items():
        if info.get("month", "") != effective_month and info.get("month", "") != "":
            if info.get("status") == "SUSPENDED":
                info["month"] = effective_month
                continue

            info["strike_count"] = 0
            info["status"] = "ACTIVE"
            info["month"] = effective_month
            reset_count += 1

    if reset_count > 0:
        msg = f"Monthly cooldown: Reset strikes for {reset_count} eligible drivers for {effective_month}"
        logger.info(msg)
        log_audit(msg)
        save_strike_state(state)

    return state


def apply_strike(driver_id, state, event_ts=None):
    """Apply a single safety strike to a driver (caps at SUSPENSION_STRIKE_LIMIT)."""
    effective_month = get_effective_month(event_ts)
    if driver_id not in state:
        state[driver_id] = {
            "strike_count": 0, "status": "ACTIVE", "month": effective_month
        }

    driver_state = state[driver_id]
    if driver_state["status"] == "SUSPENDED":
        return driver_state

    driver_state["strike_count"] = min(driver_state["strike_count"] + 1, SUSPENSION_STRIKE_LIMIT)

    if driver_state["strike_count"] >= SUSPENSION_STRIKE_LIMIT:
        driver_state["status"] = "SUSPENDED"
        msg = f"SUSPENSION: Driver {driver_id} hit {SUSPENSION_STRIKE_LIMIT} strikes and is now suspended."
        logger.warning(msg)
        log_audit(msg)

    driver_state["month"] = effective_month
    return driver_state


def calculate_adjusted_rate(base_rate, strike_count):
    """BRD: current_adjusted_rate = base_rate × (1 - 0.05 × strike_count)"""
    deduction = base_rate * PENALTY_RATE * strike_count
    return round(max(base_rate - deduction, 0.0), 2)


# ================================================================
# 3. POSTGRES EXPORT (GOLD REPORTING)
# ================================================================

def sync_to_postgres(records):
    """BRD Section 6.2 — Reporting Database via UPSERT."""
    if not psycopg2 or not records:
        return

    try:
        conn = psycopg2.connect(
            host=POSTGRES_CONFIG["HOST"],
            port=POSTGRES_CONFIG["PORT"],
            database=POSTGRES_CONFIG["DATABASE"],
            user=POSTGRES_CONFIG["USER"],
            password=POSTGRES_CONFIG["PASSWORD"],
            connect_timeout=3,
        )
        cur = conn.cursor()

        query = f"""
            INSERT INTO {POSTGRES_CONFIG["DRIVER_STATUS_TABLE"]}
            (driver_id, base_rate, strike_count, current_adjusted_rate, status, month)
            VALUES %s
            ON CONFLICT (driver_id) DO UPDATE SET
            strike_count = EXCLUDED.strike_count,
            current_adjusted_rate = EXCLUDED.current_adjusted_rate,
            status = EXCLUDED.status,
            month = EXCLUDED.month;
        """

        data = [
            (r["driver_id"], r["base_rate"], r["strike_count"],
             r["current_adjusted_rate"], r["status"], r["month"])
            for r in records
        ]

        execute_values(cur, query, data)
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Synced {len(records)} active drivers to PostgreSQL")
    except Exception as e:
        logger.debug(f"PostgreSQL sync skipped/failed: {e}")


# ================================================================
# 4. foreachBatch PROCESSOR (core micro-batch logic)
# ================================================================

# These are loaded once at startup and refreshed inside the batch processor
_ref_data = {
    "zones_df": None,
    "assignments_df": None,
    "strike_state": {},
    "last_assignment_refresh": 0,
}


def process_micro_batch(batch_df: DataFrame, batch_id: int):
    """
    Called by Spark Structured Streaming for each micro-batch.
    Implements the full BRD pipeline using PySpark DataFrames:
      1. DLQ filtering
      2. Bronze raw telemetry write
      3. Driver resolution via broadcast join with assignments
      4. Violation detection via broadcast join with restricted zones
      5. Strike tracking & penalty calculation
      6. Gold layer driver_safety_status output
      7. Postgres export
    """
    spark = batch_df.sparkSession

    if batch_df.rdd.isEmpty():
        return

    event_count = batch_df.count()
    logger.info(f"Processing micro-batch {batch_id}: {event_count} events")

    # ── Refresh reference data every 60 seconds ──────────────
    now = time.time()
    if now - _ref_data["last_assignment_refresh"] > 60:
        _ref_data["assignments_df"] = load_assignments_df(spark)
        _ref_data["last_assignment_refresh"] = now

    zones_df = _ref_data["zones_df"]
    assignments_df = _ref_data["assignments_df"]
    strike_state = _ref_data["strike_state"]

    # ── 1. DLQ: Filter events missing critical fields ────────
    valid_df = batch_df.filter(
        col("vin").isNotNull() &
        (col("vin") != "") &
        col("event_timestamp").isNotNull() &
        (col("event_timestamp") != "")
    )

    dlq_df = batch_df.subtract(valid_df)
    dlq_count = dlq_df.count()

    if dlq_count > 0:
        dlq_path = os.path.join(DLQ_DIR, f"dlq_batch_{batch_id}")
        dlq_df.write.mode("append").json(dlq_path)
        logger.warning(f"DLQ: {dlq_count} malformed events quarantined to {dlq_path}")

    if valid_df.rdd.isEmpty():
        return

    # ── 2. Parse event_timestamp to proper TimestampType ─────
    valid_df = valid_df.withColumn(
        "event_ts",
        to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss")
    )

    # ── 3. Deduplicate within batch (vin + event_timestamp) ──
    valid_df = valid_df.dropDuplicates(["vin", "event_timestamp"])

    # ── 4. Write Bronze raw telemetry (date-partitioned Parquet) ──
    bronze_df = valid_df.withColumn("event_date", to_date(col("event_ts")))
    bronze_path = TELEMETRY_RAW_DIR
    (bronze_df
     .write
     .mode("append")
     .partitionBy("event_date")
     .parquet(bronze_path))
    logger.info(f"Bronze: {bronze_df.count()} raw telemetry events written")

    # ── 5. Driver Resolution via broadcast join with assignments ──
    # BRD: "In the stream, you must join the Kafka vin with the
    # Asset History table to find the correct driver_id currently assigned."
    resolved_df = valid_df.join(
        broadcast(assignments_df),
        on="vin",
        how="left",
    )

    # Use assignment driver_id if the stream driver_id is blank/null
    resolved_df = resolved_df.withColumn(
        "resolved_driver_id",
        when(
            (col("driver_id").isNull()) | (col("driver_id") == "") | (col("driver_id") == "DRV_UNKNOWN"),
            col("assign_driver_id")
        ).otherwise(col("driver_id"))
    ).withColumn(
        "resolved_daily_rate",
        when(col("daily_rate").isNotNull(), col("daily_rate")).otherwise(lit(0.0))
    )

    # ── 6. Violation Detection ───────────────────────────────
    # 6a. Speed violation: speed > 110 km/h
    resolved_df = resolved_df.withColumn(
        "is_speeding",
        when(col("speed") > SPEED_THRESHOLD, lit(True)).otherwise(lit(False))
    )

    # 6b. Geofence violation: broadcast join with restricted zones
    # Cross join with zones, then check if lat/long falls within bounding box
    if zones_df.count() > 0:
        geo_df = resolved_df.crossJoin(broadcast(zones_df)).filter(
            (col("lat").between(col("min_lat"), col("max_lat"))) &
            (col("long").between(col("min_long"), col("max_long")))
        ).select(
            col("vin"),
            col("event_timestamp"),
            col("zone_name").alias("matched_zone_name"),
        ).dropDuplicates(["vin", "event_timestamp"])

        # Left join back: adds matched_zone_name (null if no zone breach)
        resolved_df = resolved_df.join(
            geo_df,
            on=["vin", "event_timestamp"],
            how="left",
        )
        resolved_df = resolved_df.withColumn(
            "is_in_zone",
            col("matched_zone_name").isNotNull()
        )
    else:
        resolved_df = resolved_df.withColumn("matched_zone_name", lit(None).cast(StringType()))
        resolved_df = resolved_df.withColumn("is_in_zone", lit(False))

    # 6c. Combine: is_violation = is_speeding OR is_in_zone
    resolved_df = resolved_df.withColumn(
        "is_violation",
        col("is_speeding") | col("is_in_zone")
    )

    # 6d. Classify violation_type
    resolved_df = resolved_df.withColumn(
        "violation_type",
        when(col("is_speeding") & col("is_in_zone"), lit("SPEED_VIOLATION|ZONE_INTRUSION"))
        .when(col("is_speeding"), lit("SPEED_VIOLATION"))
        .when(col("is_in_zone"), lit("ZONE_INTRUSION"))
        .otherwise(lit(None))
    )

    # ── 7. Write Violations to Parquet ───────────────────────
    violations_df = resolved_df.filter(col("is_violation") == True).select(
        col("vin"),
        col("resolved_driver_id").alias("driver_id"),
        col("speed"),
        col("lat"),
        col("long"),
        col("event_timestamp"),
        col("event_ts"),
        col("violation_type"),
        col("matched_zone_name").alias("zone_name"),
    ).withColumn("event_date", to_date(col("event_ts")))

    violations_count = violations_df.count()

    if violations_count > 0:
        (violations_df
         .write
         .mode("append")
         .partitionBy("event_date")
         .parquet(VIOLATIONS_DIR))
        logger.info(f"Violations: {violations_count} events written")

    # ── 8. Strike Tracking (stateful, driver-level) ──────────
    # Get max event_timestamp for cooldown calculation
    max_event_row = resolved_df.agg(
        spark_max(col("event_ts")).alias("max_ts")
    ).collect()[0]
    max_event_ts = max_event_row["max_ts"]
    max_event_epoch = max_event_ts.timestamp() if max_event_ts else None

    # Check for monthly cooldown transition
    strike_state = check_monthly_cooldown(strike_state, max_event_epoch)

    # Collect violation driver_ids for strike application
    if violations_count > 0:
        violation_drivers = (
            violations_df
            .filter(
                col("driver_id").isNotNull() &
                (col("driver_id") != "") &
                (col("driver_id") != "DRV_UNKNOWN")
            )
            .select("driver_id")
            .distinct()
            .collect()
        )

        # BRD: One event = ONE strike max (even if both speeding + zone)
        # We aggregate per-driver per-batch: count distinct violation events
        violation_counts = (
            violations_df
            .filter(
                col("driver_id").isNotNull() &
                (col("driver_id") != "") &
                (col("driver_id") != "DRV_UNKNOWN")
            )
            .groupBy("driver_id")
            .count()
            .collect()
        )

        for row in violation_counts:
            driver_id = row["driver_id"]
            num_violations = row["count"]
            for _ in range(num_violations):
                apply_strike(driver_id, strike_state, max_event_epoch)

    # ── 9. Write Driver Safety Status (Gold Layer) ───────────
    if strike_state:
        # Build assignment rate lookup from the Spark DataFrame
        rate_rows = assignments_df.select("assign_driver_id", "daily_rate").collect()
        driver_rates = {r["assign_driver_id"]: float(r["daily_rate"] or 0.0) for r in rate_rows}

        effective_month = get_effective_month(max_event_epoch)
        status_records = []
        for driver_id, info in strike_state.items():
            base_rate = driver_rates.get(driver_id, 0.0)
            sc = info.get("strike_count", 0)
            status_records.append({
                "driver_id": driver_id,
                "base_rate": base_rate,
                "strike_count": sc,
                "current_adjusted_rate": calculate_adjusted_rate(base_rate, sc),
                "status": info.get("status", "ACTIVE"),
                "month": info.get("month", effective_month),
            })

        # Write as Parquet using Spark
        status_df = spark.createDataFrame(status_records)
        status_path = os.path.join(SAFETY_STATUS_DIR, f"driver_safety_status_{effective_month}")
        (status_df
         .coalesce(1)
         .write
         .mode("overwrite")
         .parquet(status_path))

        # Also write CSV for human readability
        csv_path = os.path.join(SAFETY_STATUS_DIR, f"driver_safety_status_{effective_month}_csv")
        (status_df
         .coalesce(1)
         .write
         .mode("overwrite")
         .option("header", "true")
         .csv(csv_path))

        # Sync to PostgreSQL reporting layer
        sync_to_postgres(status_records)

    # ── 10. Persist strike state to disk (crash recovery) ────
    save_strike_state(strike_state)
    _ref_data["strike_state"] = strike_state

    logger.info(
        f"Micro-batch {batch_id} complete: "
        f"{event_count} events, {violations_count} violations, "
        f"{dlq_count} DLQ, {len(strike_state)} tracked drivers"
    )


# ================================================================
# 5. MAIN STREAMING ENTRY POINT
# ================================================================

def run_consumer():
    """Start the PySpark Structured Streaming Kafka consumer."""
    logger.info("OmniRoute Telemetry Consumer V2 (PySpark) Starting...")

    # ── Create SparkSession ──────────────────────────────────
    spark = (
        SparkSession.builder
        .appName("OmniRoute_TelemetryConsumer_V2")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # ── Load reference data ──────────────────────────────────
    _ref_data["zones_df"] = load_restricted_zones_df(spark)
    _ref_data["assignments_df"] = load_assignments_df(spark)
    _ref_data["strike_state"] = load_strike_state()
    _ref_data["strike_state"] = check_monthly_cooldown(_ref_data["strike_state"], None)
    _ref_data["last_assignment_refresh"] = time.time()

    # ── Read stream from Kafka ───────────────────────────────
    logger.info(f"Connecting to Kafka: {KAFKA_SERVER}, topic: {KAFKA_TOPIC}")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse Kafka value as JSON ────────────────────────────
    parsed_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_timestamp")
        .select(
            from_json(col("json_str"), TELEMETRY_SCHEMA).alias("data"),
            col("kafka_timestamp"),
        )
        .select("data.*", "kafka_timestamp")
    )

    # ── Add watermark on event_timestamp for late data handling ──
    watermarked_stream = (
        parsed_stream
        .withColumn(
            "event_ts_parsed",
            to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss")
        )
        .withWatermark("event_ts_parsed", LATE_DATA_THRESHOLD)
    )

    # ── Start the streaming query with foreachBatch ──────────
    logger.info("Starting streaming query with foreachBatch...")

    query = (
        watermarked_stream
        .writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="10 seconds")
        .start()
    )

    logger.info("Streaming query started. Waiting for termination (Ctrl+C to stop)...")
    query.awaitTermination()


# ================================================================
# ENTRY POINT
# ================================================================

if __name__ == "__main__":
    run_consumer()
