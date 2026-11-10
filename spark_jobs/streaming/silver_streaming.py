"""
===========================================================
OmniRoute Smart Logistics Engine — Silver Streaming Layer
===========================================================

Runs on EC2 as a long-running Spark Structured Streaming job.

Responsibilities (Silver — cleaning + enrichment ONLY):
  1. Read validated telemetry from Bronze S3 (streaming Parquet)
  2. SCD2 temporal join to resolve correct driver_id from vehicle_assignment
  3. Filter out unassigned vehicles (no driver_id → dropped)
  4. Deduplicate events on (vin, event_timestamp) with 10-min Watermark
  5. Geofence zone enrichment via bounding-box pre-filter + broadcast join
  6. Write enriched data to Silver S3 (date/hour-partitioned Parquet)
"""
import os
import sys
import json
import time
import logging

import boto3
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    lit, col, when, to_date, hour, current_timestamp, 
    from_unixtime, to_timestamp, broadcast, coalesce, 
    unix_timestamp, regexp_replace
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, LongType, DateType
)
import psycopg2

# ──────────────────────────────────────────────────────────────
# LOGGING & CONFIG
# ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("OmniRoute.Silver")

CURRENT_DIR  = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

IS_EMR = os.path.exists("/emr")

if not IS_EMR:
    from scripts.config import POSTGRES_CONFIG
else:
    POSTGRES_CONFIG = {
        "HOST":                "172.31.35.242",
        "PORT":                "5432",
        "DATABASE":            "omniroute_reporting",
        "USER":                "omniroute_user",
        "PASSWORD":            "OmniRoute2026!",
        "DRIVER_STATUS_TABLE": "report.driver_safety_status",
    }

try:
    if IS_EMR:
        import boto3 as _b3_init
        _s3_init = _b3_init.client("s3")
        _raw = _s3_init.get_object(
            Bucket="ttn-de-bootcamp-bronze-us-east-1",
            Key="poc-bootcamp-group5-bronze/emr/s3_paths.json"
        )["Body"].read()
        _S3 = json.loads(_raw)
        logger.info("[EMR] Loaded s3_paths.json from S3.")
    else:
        with open(os.path.join(PROJECT_ROOT, "s3_paths.json"), "r") as _f:
            _S3 = json.load(_f)
except Exception as _e:
    logger.critical(f"Cannot load s3_paths.json: {_e}")
    sys.exit(1)

def _s3_path(layer, name):
    """Full S3 prefix, ending in '/', for layers.<layer>.paths.<name> in s3_paths.json."""
    cfg = _S3["layers"][layer]
    return f"s3://{cfg['bucket']}/{cfg['prefix']}/{cfg['paths'][name]}/"


# ── S3 Paths ──
BRONZE_TELEMETRY_PATH  = _s3_path("bronze", "ingested") + "telemetry"
SILVER_TELEMETRY_PATH  = _s3_path("silver", "telemetry").rstrip("/")
CHECKPOINT_PATH_SILVER = _s3_path("bronze", "checkpoints") + "silver_streaming_parquet_v2"
VEHICLE_ASSIGNMENT_SILVER_PATH = _s3_path("silver", "vehicle_assignment").rstrip("/")

# ── Postgres Config (read-only: zones fetch) ──
PG_CONN_STR = (
    f"host={POSTGRES_CONFIG['HOST']} port={POSTGRES_CONFIG['PORT']} "
    f"dbname={POSTGRES_CONFIG['DATABASE']} user={POSTGRES_CONFIG['USER']} "
    f"password={POSTGRES_CONFIG['PASSWORD']}"
)

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

def fetch_active_zones_from_postgres() -> list:
    zones = []
    try:
        conn = psycopg2.connect(PG_CONN_STR, connect_timeout=5)
        cur  = conn.cursor()
        cur.execute("SELECT zone_name, min_lat, max_lat, min_long, max_long FROM report.restricted_zones")
        for zn, mlat, mxlat, mlong, mxlong in cur.fetchall():
            zones.append({"zone_name": zn, "min_lat": float(mlat), "max_lat": float(mxlat), "min_long": float(mlong), "max_long": float(mxlong)})
        cur.close()
        conn.close()
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

    spark = batch_df.sparkSession

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
        logger.info(f"Batch {batch_id}: Success. Input={input_count}, Output saved to Silver S3.")
    except Exception as e:
        logger.error(f"Batch {batch_id}: Write failed: {e}")

# ──────────────────────────────────────────────────────────────
# PIPELINE START
# ──────────────────────────────────────────────────────────────
def run_silver():
    global _GLOBAL_ASSIGNMENT_DF, _GLOBAL_ZONES_DF, _GLOBAL_ZONES_LIST
    
    logger.info("=" * 60)
    logger.info("Starting OmniRoute Silver Streaming Layer")
    logger.info("=" * 60)

    spark = SparkSession.builder \
        .appName("OmniRoute_Silver_Streaming") \
        .config("spark.sql.streaming.minBatchesToRetain", "2") \
        .config("spark.sql.files.ignoreMissingFiles", "true") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # ── PRODUCTION FIX: Load reference data ONCE at startup ──
    logger.info("[SILVER] Pre-loading reference data into global cache...")
    _GLOBAL_ASSIGNMENT_DF = load_assignment_table(spark)
    if _GLOBAL_ASSIGNMENT_DF: _GLOBAL_ASSIGNMENT_DF.cache()
    
    _GLOBAL_ZONES_LIST = fetch_active_zones_from_postgres()
    if _GLOBAL_ZONES_LIST:
        _GLOBAL_ZONES_DF = spark.createDataFrame(_GLOBAL_ZONES_LIST, ZONES_SCHEMA).cache()
    
    # ── Robust Startup Wait ──
    import time as _time
    _hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    _uri = spark._jvm.java.net.URI(BRONZE_TELEMETRY_PATH)
    _fs  = spark._jvm.org.apache.hadoop.fs.FileSystem.get(_uri, _hadoop_conf)
    _path_obj = spark._jvm.org.apache.hadoop.fs.Path(BRONZE_TELEMETRY_PATH)
    _max_wait = 300
    _waited   = 0
    
    status = []
    while (_waited < _max_wait):
        if _fs.exists(_path_obj):
            status = _fs.listStatus(_path_obj)
            if len(status) > 0:
                break
        logger.info(f"[SILVER] Waiting for Bronze FILES... ({_waited}s)")
        _time.sleep(10)
        _waited += 10
    
    # ── PRODUCTION FIX: Move Watermark to Stream Level ──
    CHECKPOINT_V3 = CHECKPOINT_PATH_SILVER + "_v3"
    
    bronze_stream = spark.readStream \
        .schema(BRONZE_SCHEMA) \
        .option("maxFilesPerTrigger", 5) \
        .option("ignoreMissingFiles", "true") \
        .parquet(BRONZE_TELEMETRY_PATH) \
        .withWatermark("event_ts", "10 minutes")
    
    query = bronze_stream.writeStream \
        .foreachBatch(process_silver_batch) \
        .option("checkpointLocation", CHECKPOINT_V3) \
        .trigger(processingTime="30 seconds") \
        .start()
    query.awaitTermination()

if __name__ == "__main__":
    run_silver()
