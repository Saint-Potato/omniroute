"""
===========================================================
OmniRoute Smart Logistics Engine — Gold Streaming Layer
===========================================================

Runs on EC2 as a long-running Spark Structured Streaming job.

Responsibilities (Gold — stateful streak detection + strike management):
  1. Read enriched telemetry from Silver S3 (streaming Parquet)
  2. Clean stream by joining against VR/VA (Ghost Driver fix)
  3. Calculate strikes via stateful warning accumulation (5-warning logic)
  4. Write detected strikes to TWO Postgres tables:
       a. report.driver_safety_status (upsert)
       b. report.fact_driver_strike (append-only)
  5. Write detected strikes to Gold S3 (date/hour-partitioned Parquet)

Architecture:
  - applyInPandasWithState for high-throughput stateful processing
  - 5-warning accumulation with BOTH_VIOLATION logic
  - S3 checkpointing for exactly-once crash recovery

Run on EC2:
    cd ~/omniroute
    python3 scripts/streaming_complete/gold_streaming.py
"""
import os
import sys
import json
import time
import datetime
import logging
from typing import Iterable, Tuple
import pandas as pd

import boto3
import psycopg2
import psycopg2.extras

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, lit, broadcast,
    to_date, hour, from_unixtime, to_timestamp,
    upper, trim, regexp_replace
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, DateType
)
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

# ──────────────────────────────────────────────────────────────
# LOGGING & CONFIG
# ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("OmniRoute.Gold")

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
SILVER_TELEMETRY_PATH = _s3_path("silver", "telemetry").rstrip("/")
CHECKPOINT_PATH_GOLD  = _s3_path("bronze", "checkpoints") + "gold_streaming_parquet"
GOLD_STRIKE_PATH = _s3_path("gold", "telemetry_strike").rstrip("/")
VEHICLE_ASSIGNMENT_PATH = _s3_path("silver", "vehicle_assignment").rstrip("/")
VEHICLE_REGISTRY_PATH = _s3_path("silver", "vehicle_registry").rstrip("/")

# ── Postgres Config ──
PG_CONN_STR = (
    f"host={POSTGRES_CONFIG['HOST']} port={POSTGRES_CONFIG['PORT']} "
    f"dbname={POSTGRES_CONFIG['DATABASE']} user={POSTGRES_CONFIG['USER']} "
    f"password={POSTGRES_CONFIG['PASSWORD']}"
)

# ── Constants ──
SUSPENSION_LIMIT = 10
PENALTY_RATE     = 0.05
WARNINGS_PER_STRIKE      = 3
WARNING_WINDOW_SECONDS   = 30
COOLDOWN_SECONDS         = 900
MIN_VIOLATION_DURATION   = 10
SPEED_THRESHOLD          = 110
STATE_CLEANUP_TIMEOUT_MS = 1_800_000

# ── Schemas ──
SILVER_SCHEMA = StructType([
    StructField("vin", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("speed", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("long", DoubleType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("event_unix", DoubleType(), True),
    StructField("bronze_ingested_at", TimestampType(), True),
    StructField("matched_zone_name", StringType(), True),
    StructField("silver_processed_at", TimestampType(), True),
    StructField("date", DateType(), True),
    StructField("hour", IntegerType(), True),
])

STRIKE_STATE_SCHEMA = StructType([
    StructField("spd_cnt",        IntegerType(), False),
    StructField("spd_first",      DoubleType(),  False),
    StructField("spd_last",       DoubleType(),  False),
    StructField("zone_cnt",       IntegerType(), False),
    StructField("zone_first",     DoubleType(),  False),
    StructField("zone_last",      DoubleType(),  False),
    StructField("strike_count",   IntegerType(), False),
    StructField("last_strike_ts", DoubleType(),  False),
])

STRIKE_OUTPUT_SCHEMA = StructType([
    StructField("driver_id",      StringType(), False),
    StructField("vin",            StringType(), False),
    StructField("violation_type", StringType(), False),
    StructField("start_time",     DoubleType(), False),
    StructField("duration_sec",   DoubleType(), False),
])

# ──────────────────────────────────────────────────────────────
# CLEAN STREAM — SAFE ENRICHMENT (CORRECTED)
# ──────────────────────────────────────────────────────────────
def clean_stream(batch_df: DataFrame, spark: SparkSession) -> DataFrame:
    try:
        # 1. Fetch only what we need (daily_rate). 
        # Production-grade fix: Use upper/trim on VIN and join only on VIN to avoid nulls from strict matching.
        va_df = spark.read.format("parquet").load(VEHICLE_ASSIGNMENT_PATH) \
            .select(
                upper(trim(col("vin"))).alias("va_vin"),
                regexp_replace(col("daily_rate"), '"', '').cast("double").alias("daily_rate")
            ).dropDuplicates(["va_vin"])

        # 2. Clean the incoming batch VIN for matching
        clean_batch = batch_df.withColumn(
            "join_vin",
            upper(trim(col("vin")))
        )

        # 3. Use an INNER join to guarantee only assigned vehicles are processed.
        joined = clean_batch.join(
            broadcast(va_df),
            clean_batch["join_vin"] == va_df["va_vin"],
            "inner"
        ).drop("join_vin", "va_vin")
            
        return joined
        
    except Exception as e:
        logger.warning(f"[CLEAN] Could not fetch daily_rate reference data: {e}. Passing through.")
        return batch_df

# ──────────────────────────────────────────────────────────────
# STATEFUL STRIKE DETECTION — 5-WARNING ACCUMULATION
# ──────────────────────────────────────────────────────────────
def track_driver_strikes(key: Tuple[str, str], pdfs: Iterable[pd.DataFrame], state: GroupState) -> Iterable[pd.DataFrame]:
    driver_id, vin = key[0], key[1]
    out_records = []
    _EMPTY = pd.DataFrame(columns=["driver_id", "vin", "violation_type", "start_time", "duration_sec"])

    if state.hasTimedOut:
        if state.exists:
            state.remove()
        yield _EMPTY
        return

    if state.exists:
        s = state.get
        spd_cnt, spd_first, spd_last = s[0], s[1], s[2]
        zone_cnt, zone_first, zone_last = s[3], s[4], s[5]
        strike_count, last_strike_ts = s[6], s[7]
    else:
        spd_cnt, spd_first, spd_last = 0, 0.0, 0.0
        zone_cnt, zone_first, zone_last = 0, 0.0, 0.0
        strike_count, last_strike_ts = 0, 0.0

    for pdf in pdfs:
        if pdf.empty: continue
        pdf = pdf.sort_values("event_unix")
        for _, row in pdf.iterrows():
            speed  = float(row["speed"])
            evt_ts = float(row["event_unix"])
            matched_zone = row.get("matched_zone_name", None)
            is_in_zone = pd.notna(matched_zone) and str(matched_zone) != "" and str(matched_zone) != "nan"
            is_speeding = speed >= SPEED_THRESHOLD

            if not is_speeding and not is_in_zone: continue
            # ── 3. Check for Cooldown (Strict) ──
            # Only process violations if we are outside the 15-minute strike cooldown
            if last_strike_ts > 0 and (evt_ts - last_strike_ts) < COOLDOWN_SECONDS:
                continue

            # ── 4. Accumulate Warnings ──
            triggered_speed = False
            triggered_zone = False

            if is_speeding:
                # Reset counter if the last warning was too long ago (60s window)
                if spd_cnt > 0 and (evt_ts - spd_last) > WARNING_WINDOW_SECONDS:
                    spd_cnt, spd_first, spd_last = 0, 0.0, 0.0
                
                spd_cnt += 1
                if spd_cnt == 1: spd_first = evt_ts
                spd_last = evt_ts
                
                if spd_cnt >= WARNINGS_PER_STRIKE and (spd_last - spd_first) >= MIN_VIOLATION_DURATION:
                    triggered_speed = True

            if is_in_zone:
                if zone_cnt > 0 and (evt_ts - zone_last) > WARNING_WINDOW_SECONDS:
                    zone_cnt, zone_first, zone_last = 0, 0.0, 0.0
                
                zone_cnt += 1
                if zone_cnt == 1: zone_first = evt_ts
                zone_last = evt_ts
                
                if zone_cnt >= WARNINGS_PER_STRIKE and (zone_last - zone_first) >= MIN_VIOLATION_DURATION:
                    triggered_zone = True

            # ── 5. Trigger Finalized Strike ──
            if triggered_speed or triggered_zone:
                # Prioritize BOTH, then ZONE, then SPEED
                if triggered_speed and triggered_zone:
                    v_type = "BOTH_VIOLATION"
                    st = min(spd_first, zone_first)
                    dur = max(spd_last, zone_last) - st
                elif triggered_zone:
                    v_type = "ZONE_VIOLATION"
                    st = zone_first
                    dur = zone_last - zone_first
                else:
                    v_type = "SPEED_VIOLATION"
                    st = spd_first
                    dur = spd_last - spd_first

                out_records.append((driver_id, vin, v_type, st, dur))
                
                # IMPORTANT: Reset all state after a strike and update cooldown
                strike_count += 1
                last_strike_ts = evt_ts
                spd_cnt, spd_first, spd_last = 0, 0.0, 0.0
                zone_cnt, zone_first, zone_last = 0, 0.0, 0.0

    state.update((spd_cnt, spd_first, spd_last, zone_cnt, zone_first, zone_last, strike_count, last_strike_ts))
    state.setTimeoutDuration(STATE_CLEANUP_TIMEOUT_MS)

    if out_records:
        yield pd.DataFrame(out_records, columns=_EMPTY.columns)
    else:
        yield _EMPTY

# ──────────────────────────────────────────────────────────────
# POSTGRES HELPERS
# ──────────────────────────────────────────────────────────────
_PG_CONN = None

def _get_pg_connection():
    global _PG_CONN
    try:
        if _PG_CONN and not _PG_CONN.closed:
            _PG_CONN.cursor().execute("SELECT 1")
            return _PG_CONN
    except: pass
    _PG_CONN = psycopg2.connect(PG_CONN_STR, connect_timeout=5)
    return _PG_CONN

def upsert_driver_safety_status(strike_rows: list[dict]):
    if not strike_rows: return
    sql = """
        INSERT INTO report.driver_safety_status
            (driver_id, base_rate, strike_count, current_adjusted_rate, status, month)
        VALUES
            (%(driver_id)s, %(base_rate)s, 1,
             %(base_rate)s - 1 * {penalty} * %(base_rate)s,
             'ACTIVE',
             DATE_TRUNC('month', CURRENT_DATE))
        ON CONFLICT (driver_id,month) DO UPDATE SET
            strike_count = LEAST(10, report.driver_safety_status.strike_count + 1),
            base_rate = CASE WHEN EXCLUDED.base_rate > 0 THEN EXCLUDED.base_rate ELSE report.driver_safety_status.base_rate END,
            current_adjusted_rate = GREATEST(0,
                CASE WHEN EXCLUDED.base_rate > 0 THEN EXCLUDED.base_rate ELSE report.driver_safety_status.base_rate END
                - LEAST(10, (report.driver_safety_status.strike_count + 1)) * {penalty}
                * CASE WHEN EXCLUDED.base_rate > 0 THEN EXCLUDED.base_rate ELSE report.driver_safety_status.base_rate END
            ),
            status = CASE
                WHEN report.driver_safety_status.strike_count + 1 >= {limit} THEN 'SUSPENDED'
                ELSE report.driver_safety_status.status
            END,
            month = DATE_TRUNC('month', CURRENT_DATE)
    """.format(penalty=PENALTY_RATE, limit=SUSPENSION_LIMIT)
    try:
        conn = _get_pg_connection()
        cur = conn.cursor()
        psycopg2.extras.execute_batch(cur, sql, strike_rows, page_size=500)
        conn.commit()
        cur.close()
        logger.info(f"[PG] Upserted {len(strike_rows)} strike(s) to report.driver_safety_status.")
    except Exception as exc:
        logger.error(f"[PG] Failed to upsert driver_safety_status: {exc}")

def insert_fact_driver_strike(strike_rows: list[dict]):
    if not strike_rows: return
    sql = """
        INSERT INTO report.fact_driver_strike
            (driver_id, timestamp, violation_type)
        VALUES
            (%(driver_id)s, %(timestamp)s, %(violation_type)s)
    """
    try:
        conn = _get_pg_connection()
        cur = conn.cursor()
        psycopg2.extras.execute_batch(cur, sql, strike_rows, page_size=500)
        conn.commit()
        cur.close()
        logger.info(f"[PG] Inserted {len(strike_rows)} strike(s) to report.fact_driver_strike.")
    except Exception as exc:
        logger.error(f"[PG] Failed to insert fact_driver_strike: {exc}")

# ──────────────────────────────────────────────────────────────
# MICRO-BATCH PROCESSOR
# ──────────────────────────────────────────────────────────────
def process_gold_batch(batch_df: DataFrame, batch_id: int):
    # Cache input to avoid re-triggering UDF
    batch_df.cache()
    
    spark = batch_df.sparkSession
    
    # 1. Enrichment (Fetch daily_rate via Left Join)
    # Using clean_stream to ensure we have rate data for the fact table
    enriched_df = clean_stream(batch_df, spark).cache()
    
    if enriched_df.count() == 0:
        batch_df.unpersist()
        enriched_df.unpersist()
        return

    logger.info(f"{'='*60}")
    logger.info(f"Processing Gold micro-batch {batch_id}...")

    # 2. High-Performance Database Write (Parallel via Executors)
    def write_to_postgres(partition_iterator):
        # PRODUCTION FIX: Connection per partition for safety
        try:
            conn = psycopg2.connect(PG_CONN_STR, connect_timeout=5)
            cur  = conn.cursor()
            
            status_batch = []
            fact_batch = []
            
            for row in partition_iterator:
                # Safety: Ensure start_time is valid
                strike_ts_raw = row["start_time"]
                strike_ts = datetime.datetime.utcfromtimestamp(float(strike_ts_raw)) if strike_ts_raw and strike_ts_raw > 0 else datetime.datetime.utcnow()

                # Enrichment: Get base rate
                rate_raw = row["daily_rate"] if row["daily_rate"] else 0.0
                try:
                    base_rate = float(rate_raw)
                except:
                    base_rate = 0.0

                status_batch.append({"driver_id": row["driver_id"], "base_rate": base_rate})
                fact_batch.append({
                    "driver_id": row["driver_id"],
                    "timestamp": strike_ts,
                    "violation_type": row["violation_type"]
                })
                
                if len(status_batch) >= 500:
                    _flush_batches(cur, status_batch, fact_batch)
                    conn.commit()
                    status_batch, fact_batch = [], []

            if status_batch:
                _flush_batches(cur, status_batch, fact_batch)
                conn.commit()
                
            cur.close()
            conn.close()
        except Exception as e:
            import traceback

            logger.error(f"[PG] Executor write failed: {str(e)}")
            logger.error(traceback.format_exc())

            raise

    def _flush_batches(cur, status_rows, fact_rows):
        # SQL logic moved here for partition safety
        from psycopg2.extras import execute_batch
        
        # 1. Upsert Safety Status
        sql_status = f"""
            INSERT INTO report.driver_safety_status
                (driver_id, base_rate, strike_count, current_adjusted_rate, status, month)
            VALUES
                (%(driver_id)s, %(base_rate)s, 1,
                 %(base_rate)s - 0.05 * %(base_rate)s,
                 'ACTIVE', DATE_TRUNC('month', CURRENT_DATE))
            ON CONFLICT (driver_id, month) DO UPDATE SET
                strike_count = LEAST(10, report.driver_safety_status.strike_count + 1),
                current_adjusted_rate = GREATEST(0, report.driver_safety_status.base_rate - 
                    LEAST(10, (report.driver_safety_status.strike_count + 1)) * 0.05 * report.driver_safety_status.base_rate),
                status = CASE WHEN report.driver_safety_status.strike_count + 1 >= 10 THEN 'SUSPENDED' ELSE report.driver_safety_status.status END,
                month = DATE_TRUNC('month', CURRENT_DATE)
        """
        execute_batch(cur, sql_status, status_rows)
        
        # 2. Insert Fact
        sql_fact = """
            INSERT INTO report.fact_driver_strike
            (driver_id, event_timestamp, violation_type)
            VALUES
            (%(driver_id)s, %(timestamp)s, %(violation_type)s)
        """
        execute_batch(cur, sql_fact, fact_rows)

    # Parallel Execute
    try:
        enriched_df.foreachPartition(write_to_postgres)
        logger.info(f"Batch {batch_id}: Database write complete.")
    except Exception as e:
        logger.error(f"Batch {batch_id}: Database write failed: {e}")
        raise

    # 3. S3 Parquet Write
    try:
        gold_out_df = enriched_df \
            .withColumn("timestamp", to_timestamp(from_unixtime(col("start_time").cast("long")))) \
            .withColumn("date", to_date(col("timestamp"))) \
            .withColumn("hour", hour(col("timestamp"))) \
            .withColumn("gold_processed_at", current_timestamp()) \
            .coalesce(2) # PRODUCTION FIX: Coalesce to prevent small files
            
        gold_out_df.write.mode("append").partitionBy("date", "hour").parquet(GOLD_STRIKE_PATH)
        logger.info(f"Batch {batch_id}: Success. Strikes persisted to Gold S3.")
    except Exception as e:
        logger.error(f"Batch {batch_id}: Gold S3 write failed: {e}")

    batch_df.unpersist()
    enriched_df.unpersist()

# ──────────────────────────────────────────────────────────────
# GOLD PIPELINE
# ──────────────────────────────────────────────────────────────
def run_gold():
    logger.info("=" * 60)
    logger.info("Starting OmniRoute Gold Streaming Layer (EC2 Spark)")
    logger.info("=" * 60)
    
    builder = (
        SparkSession.builder
        .appName("OmniRoute_Gold_Streaming")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.streaming.minBatchesToRetain", "2")
    )

    if IS_EMR:
        logger.info("[ENV] Running on EMR — EMRFS handles s3:// natively.")
    else:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3.impl",  "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .config("spark.sql.sources.commitProtocolClass",
                "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .config("spark.hadoop.fs.s3a.committer.name", "directory")
        )
        logger.info("[ENV] Running on EC2.")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import time as _time
    _hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    _uri = spark._jvm.java.net.URI(SILVER_TELEMETRY_PATH)
    _fs  = spark._jvm.org.apache.hadoop.fs.FileSystem.get(_uri, _hadoop_conf)
    _path_obj = spark._jvm.org.apache.hadoop.fs.Path(SILVER_TELEMETRY_PATH)
    _max_wait = 600
    _waited   = 0
    logger.info(f"[GOLD] Checking for Silver source path: {SILVER_TELEMETRY_PATH}")
    
    status = []
    while (_waited < _max_wait):
        if _fs.exists(_path_obj):
            status = _fs.listStatus(_path_obj)
            if len(status) > 0:
                break
        logger.info(f"[GOLD] Waiting for Silver FILES... ({_waited}s)")
        _time.sleep(10)
        _waited += 10
    
    logger.info(f"[DEBUG] Files in Silver: {len(status)}")

    # ── Updated Checkpoint for v2 ──
    CHECKPOINT_V2 = CHECKPOINT_PATH_GOLD + "_v2"

    silver_stream = spark.readStream \
        .schema(SILVER_SCHEMA) \
        .option("maxFilesPerTrigger", 1) \
        .option("ignoreMissingFiles", "true") \
        .option("failOnDataLoss", "false") \
        .parquet(SILVER_TELEMETRY_PATH) \
        .withWatermark("event_ts", "10 minutes")

    gold_query = silver_stream \
        .repartition(10, "driver_id", "vin") \
        .groupBy("driver_id", "vin") \
        .applyInPandasWithState(
            func=track_driver_strikes,
            outputStructType=STRIKE_OUTPUT_SCHEMA,
            stateStructType=STRIKE_STATE_SCHEMA,
            outputMode="append",
            timeoutConf="ProcessingTimeTimeout"
        ) \
        .writeStream \
        .foreachBatch(process_gold_batch) \
        .option("checkpointLocation", CHECKPOINT_V2) \
        .trigger(processingTime="10 seconds") \
        .start()

    logger.info("Streaming query started. Listening for new Silver files. Press Ctrl+C to stop.")
    gold_query.awaitTermination()


if __name__ == "__main__":
    run_gold()
