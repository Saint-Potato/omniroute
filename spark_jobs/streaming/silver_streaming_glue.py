"""
===========================================================
OmniRoute Smart Logistics Engine — Silver Streaming Layer (AWS Glue)
===========================================================

Glue port of silver_streaming.py (the EMR/EC2 version). Same Silver logic,
same output path and columns, so gold_streaming.py reads it unchanged:

  1. Read validated telemetry from Bronze S3 (streaming Parquet)
  2. SCD2 temporal join to resolve correct driver_id from vehicle_assignment
  3. Filter out unassigned vehicles (no driver_id → dropped)
  4. Deduplicate events on (vin, event_timestamp) within each micro-batch
  5. Geofence zone enrichment via bounding-box pre-filter + broadcast join
  6. Write enriched data to Silver S3 (date/hour-partitioned Parquet)

What changed for Glue:
  - Paths and PostgreSQL settings come from job parameters instead of
    s3_paths.json, scripts/config.py and the password written in the code.
  - Restricted zones are read with Spark's JDBC reader (the PostgreSQL
    driver ships with Glue) instead of psycopg2, which isn't installed and
    can't be pip-installed by a job inside a VPC without internet access.
  - --trigger_mode available_now (default) processes every Bronze file not
    yet seen, writes it and stops. --trigger_mode continuous runs a
    micro-batch every --trigger_interval_seconds, like the EMR version.

Kept as in the EMR version (see the round-2 test plan, problems 5 and 8):
  - The Silver vehicle_assignment Delta table is read as plain Parquet.
  - Zones are read from report.restricted_zones (override with --zones_table).
  - Reference data is loaded once when the job starts.

Job type: Spark (glueetl), Glue 5.0. Needs a Glue network connection that
reaches PostgreSQL (e.g. omniroute-pg).

Glue job parameters:
  Required
    --bronze_telemetry_path     s3://.../ingested/telemetry (bronze_streaming_glue.py output)
    --silver_telemetry_path     s3://.../silver/telemetry
    --checkpoint_path           s3://.../checkpoints/silver_telemetry_glue
    --vehicle_assignment_path   s3://.../silver/silver.vehicle_assignment/
    --pg_host, --pg_port, --pg_database, --pg_user, --pg_password
  Optional
    --zones_table               default report.restricted_zones
    --max_files_per_trigger     default 5
    --bronze_wait_seconds       default 300 (how long to wait for the first Bronze files)
    --trigger_mode              available_now (default) | continuous
    --trigger_interval_seconds  default 30 (continuous mode only)
"""
import sys
import time
import logging

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    lit, col, when, to_date, hour, current_timestamp,
    broadcast, coalesce, regexp_replace
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, DateType
)

# ──────────────────────────────────────────────────────────────
# LOGGING & CONFIG
# ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("OmniRoute.Silver")

REQUIRED_PARAMS = [
    "JOB_NAME",
    "bronze_telemetry_path",
    "silver_telemetry_path",
    "checkpoint_path",
    "vehicle_assignment_path",
    "pg_host",
    "pg_port",
    "pg_database",
    "pg_user",
    "pg_password",
]
OPTIONAL_PARAMS = {
    "zones_table": "report.restricted_zones",
    "max_files_per_trigger": "5",
    "bronze_wait_seconds": "300",
    "trigger_mode": "available_now",
    "trigger_interval_seconds": "30",
}

# getResolvedOptions fails on any listed name that wasn't passed, so only
# ask for the optional ones that are present.
_passed = [name for name in OPTIONAL_PARAMS if f"--{name}" in sys.argv]
args = {**OPTIONAL_PARAMS, **getResolvedOptions(sys.argv, REQUIRED_PARAMS + _passed)}

# ── S3 Paths ──
BRONZE_TELEMETRY_PATH          = args["bronze_telemetry_path"].rstrip("/")
SILVER_TELEMETRY_PATH          = args["silver_telemetry_path"].rstrip("/")
CHECKPOINT_PATH_SILVER         = args["checkpoint_path"].rstrip("/")
VEHICLE_ASSIGNMENT_SILVER_PATH = args["vehicle_assignment_path"].rstrip("/")

# ── Postgres Config (read-only: zones fetch) ──
JDBC_URL = f"jdbc:postgresql://{args['pg_host']}:{args['pg_port']}/{args['pg_database']}"
JDBC_PROPS = {
    "user": args["pg_user"],
    "password": args["pg_password"],
    "driver": "org.postgresql.Driver",
    "sslmode": "require",
    "connectTimeout": "5",
    "loginTimeout": "5",
}
ZONES_TABLE = args["zones_table"]

TRIGGER_MODE = args["trigger_mode"].strip().lower()
if TRIGGER_MODE not in ("available_now", "continuous"):
    logger.critical(f"--trigger_mode must be available_now or continuous, got '{TRIGGER_MODE}'")
    sys.exit(1)

# ── Schemas ──
ZONES_SCHEMA = StructType([
    StructField("zone_name", StringType(), False),
    StructField("min_lat",   DoubleType(), False),
    StructField("max_lat",   DoubleType(), False),
    StructField("min_long",  DoubleType(), False),
    StructField("max_long",  DoubleType(), False),
])

BRONZE_SCHEMA = StructType([
    StructField("vin",               StringType(),    True),
    StructField("driver_id",         StringType(),    True),
    StructField("speed",             DoubleType(),    True),
    StructField("lat",               DoubleType(),    True),
    StructField("long",              DoubleType(),    True),
    StructField("event_timestamp",   StringType(),    True),
    StructField("event_ts",          TimestampType(), True),
    StructField("event_unix",        DoubleType(),    True),
    StructField("bronze_ingested_at",TimestampType(), True),
    StructField("date",              DateType(),      True),
    StructField("hour",              IntegerType(),   True),
])

# ──────────────────────────────────────────────────────────────
# SCD2 VEHICLE ASSIGNMENT LOADER (Native Caching)
# ──────────────────────────────────────────────────────────────
def load_assignment_table(spark: SparkSession) -> DataFrame:
    """
    Loads and caches the SCD2 assignment table natively in Spark.
    No .collect() used to prevent driver OOM.
    """
    try:
        raw_df = spark.read.format("parquet").load(VEHICLE_ASSIGNMENT_SILVER_PATH)
        processed_df = raw_df.filter(col("status") == "IN-TRANSIT").select(
            col("vin").alias("a_vin"),
            col("driver_id").alias("scd2_driver_id"),
            col("start_datetime").cast("timestamp"),
            col("end_datetime").cast("timestamp"),
            regexp_replace(col("daily_rate"), '"', '')
                .cast("double")
                .alias("daily_rate")
        )
        processed_df.cache()
        return processed_df
    except Exception as e:
        logger.error(f"Failed to load assignment table: {e}")
        return None

def fetch_active_zones_from_postgres(spark: SparkSession) -> list:
    zones = []
    try:
        query = f"(SELECT zone_name, min_lat, max_lat, min_long, max_long FROM {ZONES_TABLE}) AS zones"
        rows = spark.read.jdbc(url=JDBC_URL, table=query, properties=JDBC_PROPS).collect()
        for zn, mlat, mxlat, mlong, mxlong in rows:
            zones.append({"zone_name": zn, "min_lat": float(mlat), "max_lat": float(mxlat), "min_long": float(mlong), "max_long": float(mxlong)})
        logger.info(f"[ZONES] Loaded {len(zones)} zones from {ZONES_TABLE}")
    except Exception as exc:
        logger.error(f"[ZONES] Postgres fetch failed: {exc}")
    return zones

# ──────────────────────────────────────────────────────────────
# GLOBAL REFERENCE DATA (CACHED)
# ──────────────────────────────────────────────────────────────
_GLOBAL_ASSIGNMENT_DF = None
_GLOBAL_ZONES_DF      = None
_GLOBAL_ZONES_LIST    = None

def process_silver_batch(batch_df: DataFrame, batch_id: int):
    batch_start = time.time()
    input_count = batch_df.count()
    if input_count == 0:
        logger.warning("⚠️ Silver received NO data from Bronze")
        return

    logger.info("=" * 60)
    logger.info(f"Processing Silver micro-batch {batch_id}...")

    # 1. SCD2 Temporal Join (Resolve driver_id) - Uses Global Cache
    if _GLOBAL_ASSIGNMENT_DF:
        joined_df = batch_df.join(
            broadcast(_GLOBAL_ASSIGNMENT_DF),
            (batch_df["vin"] == _GLOBAL_ASSIGNMENT_DF["a_vin"]) &
            (batch_df["event_ts"] >= _GLOBAL_ASSIGNMENT_DF["start_datetime"]) &
            (batch_df["event_ts"] < coalesce(_GLOBAL_ASSIGNMENT_DF["end_datetime"], lit("2099-12-31").cast("timestamp"))),
            "left"
        )
        resolved_df = joined_df.withColumn(
            "driver_id",
            when(col("scd2_driver_id").isNotNull(), col("scd2_driver_id")).otherwise(col("driver_id"))
        ).drop("a_vin", "scd2_driver_id", "start_datetime", "end_datetime")
    else:
        resolved_df = batch_df

    # 2. Deduplication (Watermark is now at the source level)
    deduped_df = resolved_df.dropDuplicates(["vin", "event_timestamp"])

    # 3. Filter Unassigned
    cleaned_df = deduped_df.filter(col("driver_id").isNotNull() & (col("driver_id") != "DRV_UNKNOWN"))

    # 4. Geofence Enrichment - Uses Global Cache
    if _GLOBAL_ZONES_DF:
        global_min_lat = min(z["min_lat"] for z in _GLOBAL_ZONES_LIST)
        global_max_lat = max(z["max_lat"] for z in _GLOBAL_ZONES_LIST)
        global_min_long = min(z["min_long"] for z in _GLOBAL_ZONES_LIST)
        global_max_long = max(z["max_long"] for z in _GLOBAL_ZONES_LIST)

        candidates_df = cleaned_df.filter(
            (col("lat").between(global_min_lat, global_max_lat)) &
            (col("long").between(global_min_long, global_max_long))
        )

        geo_hits = candidates_df.crossJoin(broadcast(_GLOBAL_ZONES_DF)) \
            .filter(col("lat").between(col("min_lat"), col("max_lat")) & col("long").between(col("min_long"), col("max_long"))) \
            .select("vin", "event_timestamp", col("zone_name").alias("matched_zone_name")) \
            .dropDuplicates(["vin", "event_timestamp"])

        enriched_df = cleaned_df.join(geo_hits, on=["vin", "event_timestamp"], how="left")
    else:
        enriched_df = cleaned_df.withColumn("matched_zone_name", lit(None).cast("string"))

    # 5. Write to Silver S3
    try:
        final_df = enriched_df \
            .withColumn("date", to_date(col("event_ts"))) \
            .withColumn("hour", hour(col("event_ts"))) \
            .withColumn("silver_processed_at", current_timestamp()) \
            .select(
                "vin", "driver_id", "speed", "lat", "long",
                "event_timestamp", "event_ts", "event_unix",
                "bronze_ingested_at", "matched_zone_name",
                "silver_processed_at", "date", "hour"
            )

        # Optimization: Use coalesce(2) instead of repartition (Fix #4)
        final_df = final_df.coalesce(2)

        final_df.write.mode("append").partitionBy("date", "hour").parquet(SILVER_TELEMETRY_PATH)
        logger.info(f"Batch {batch_id}: Success. Input={input_count}, Output saved to Silver S3. "
                    f"({time.time() - batch_start:.1f}s)")
    except Exception as e:
        # Kept from the EMR version: a failed write is logged and the batch
        # still counts as processed, so its events are not re-read.
        logger.error(f"Batch {batch_id}: Write failed: {e}")

# ──────────────────────────────────────────────────────────────
# PIPELINE START
# ──────────────────────────────────────────────────────────────
def run_silver(spark: SparkSession):
    global _GLOBAL_ASSIGNMENT_DF, _GLOBAL_ZONES_DF, _GLOBAL_ZONES_LIST

    logger.info("=" * 60)
    logger.info("Starting OmniRoute Silver Streaming Layer (AWS Glue)")
    logger.info("=" * 60)
    logger.info(f"  Bronze S3     : {BRONZE_TELEMETRY_PATH}")
    logger.info(f"  Silver S3     : {SILVER_TELEMETRY_PATH}")
    logger.info(f"  Assignments   : {VEHICLE_ASSIGNMENT_SILVER_PATH}")
    logger.info(f"  Zones         : {ZONES_TABLE} @ {args['pg_host']}")
    logger.info(f"  Checkpoint    : {CHECKPOINT_PATH_SILVER}")
    logger.info(f"  Trigger       : {TRIGGER_MODE}")

    spark.conf.set("spark.sql.streaming.minBatchesToRetain", "2")
    spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")

    # ── PRODUCTION FIX: Load reference data ONCE at startup ──
    logger.info("[SILVER] Pre-loading reference data into global cache...")
    _GLOBAL_ASSIGNMENT_DF = load_assignment_table(spark)
    if _GLOBAL_ASSIGNMENT_DF: _GLOBAL_ASSIGNMENT_DF.cache()

    _GLOBAL_ZONES_LIST = fetch_active_zones_from_postgres(spark)
    if _GLOBAL_ZONES_LIST:
        _GLOBAL_ZONES_DF = spark.createDataFrame(_GLOBAL_ZONES_LIST, ZONES_SCHEMA).cache()

    # ── Robust Startup Wait ──
    _hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    _uri = spark._jvm.java.net.URI(BRONZE_TELEMETRY_PATH)
    _fs  = spark._jvm.org.apache.hadoop.fs.FileSystem.get(_uri, _hadoop_conf)
    _path_obj = spark._jvm.org.apache.hadoop.fs.Path(BRONZE_TELEMETRY_PATH)
    _max_wait = int(args["bronze_wait_seconds"])
    _waited   = 0

    while (_waited < _max_wait):
        if _fs.exists(_path_obj):
            status = _fs.listStatus(_path_obj)
            if len(status) > 0:
                break
        logger.info(f"[SILVER] Waiting for Bronze FILES... ({_waited}s)")
        time.sleep(10)
        _waited += 10

    # ── PRODUCTION FIX: Move Watermark to Stream Level ──
    bronze_stream = spark.readStream \
        .schema(BRONZE_SCHEMA) \
        .option("maxFilesPerTrigger", int(args["max_files_per_trigger"])) \
        .option("ignoreMissingFiles", "true") \
        .parquet(BRONZE_TELEMETRY_PATH) \
        .withWatermark("event_ts", "10 minutes")

    writer = bronze_stream.writeStream \
        .foreachBatch(process_silver_batch) \
        .option("checkpointLocation", CHECKPOINT_PATH_SILVER)

    if TRIGGER_MODE == "available_now":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{int(args['trigger_interval_seconds'])} seconds")

    query = writer.start()
    logger.info("Streaming query started.")
    query.awaitTermination()
    logger.info(f"Streaming query finished. Last progress: {query.lastProgress}")


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    spark.sparkContext.setLogLevel("WARN")

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    try:
        run_silver(spark)
    finally:
        job.commit()
