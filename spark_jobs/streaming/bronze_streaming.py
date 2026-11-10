"""
===========================================================
OmniRoute Smart Logistics Engine — Bronze Streaming Layer
===========================================================

Runs on EC2 as a long-running Spark Structured Streaming job.

Responsibilities (Bronze ONLY — no business logic):
  1. Read raw telemetry JSON from Kafka
  2. Validate schema (DLQ any malformed records)
  3. Append valid raw events to S3 Bronze (date/hour-partitioned Parquet)
  4. Send DLQ records to Kafka DLQ topic AND S3 quarantine

Architecture:
  - foreachBatch for micro-batch processing with detailed logging
  - S3 checkpointing for exactly-once crash recovery
  - SQLHadoopMapReduceCommitProtocol for atomic S3 writes
  - DataFrame caching to avoid redundant Spark actions

Run on EC2:
    cd ~/omniroute
    python3 scripts/streaming_complete/bronze_streaming.py
"""
import os
import sys
import json
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, unix_timestamp, lit,
    to_date, hour, current_timestamp, struct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# ──────────────────────────────────────────────────────────────
# LOGGING & CONFIG
# ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("OmniRoute.Bronze")

CURRENT_DIR  = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ── Environment Detection ──
_IS_EMR = os.path.exists("/emr")

try:
    if _IS_EMR:
        import boto3 as _b3_init
        _raw = _b3_init.client("s3").get_object(
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
# Using telemetry2 to avoid clashes, but keeping Parquet format
BRONZE_TELEMETRY_PATH = _s3_path("bronze", "ingested") + "telemetry"
CHECKPOINT_PATH_BRONZE = _s3_path("bronze", "checkpoints") + "telemetry_parquet"
DLQ_QUARANTINE_PATH = _s3_path("bronze", "quarantine") + "telemetry_dlq"

# ── Kafka Config ──
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "172.31.65.131:9092")
KAFKA_TOPIC  = "vehicle_telemetry_topic"
DLQ_TOPIC    = "omniroute-telemetry-dlq"

# ── Telemetry Schema (matches telemetry_producer.py output exactly) ──
TELEMETRY_SCHEMA = StructType([
    StructField("vin", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("speed", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("long", DoubleType(), True),
    StructField("event_timestamp", StringType(), True),
])

# ──────────────────────────────────────────────────────────────
# MICRO-BATCH PROCESSOR
# ──────────────────────────────────────────────────────────────
def process_bronze_batch(batch_df: DataFrame, batch_id: int):
    """
    Called by Spark Structured Streaming for each micro-batch.
    Pure Bronze logic: parse → validate → DLQ → write raw Parquet to S3.
    """
    if batch_df.count() == 0:
        logger.info(f"Batch {batch_id}: Empty batch, skipping.")
        return

    logger.info(f"{'='*60}")
    logger.info(f"Processing Bronze micro-batch {batch_id}...")

    # ── 1. Parse Kafka value (raw bytes) as JSON ──
    parsed_df = batch_df.select(
        col("value").cast("string").alias("raw_payload"),
        from_json(col("value").cast("string"), TELEMETRY_SCHEMA).alias("data")
    )

    # ── 2. Split: malformed vs valid ──
    malformed_df = parsed_df.filter(
        col("data.vin").isNull()             | (col("data.vin") == "")             |
        col("data.event_timestamp").isNull() | (col("data.event_timestamp") == "") |
        col("data.speed").isNull()           | (col("data.speed") < 0)             | (col("data.speed") > 300)
    )

    valid_df = parsed_df.filter(
        col("data.vin").isNotNull()             & (col("data.vin") != "") &
        col("data.event_timestamp").isNotNull() & (col("data.event_timestamp") != "") &
        col("data.speed").isNotNull()           & (col("data.speed") >= 0) & (col("data.speed") <= 300)
    ).select("data.*")

    # Cache both to avoid recomputing the same DAG multiple times
    malformed_df.cache()
    valid_df.cache()

    dlq_count   = malformed_df.count()
    valid_count = valid_df.count()
    total_count = dlq_count + valid_count

    logger.info(f"Batch {batch_id} → Total: {total_count}  |  Valid: {valid_count}  |  DLQ: {dlq_count}")

    # ── 3. DLQ: route bad records to Kafka + S3 quarantine ──
    if dlq_count > 0:
        logger.warning(f"Batch {batch_id}: Routing {dlq_count} malformed records to DLQ.")

        # Format with error metadata for observability
        dlq_formatted = malformed_df.select(
            struct(
                lit("Missing critical fields (VIN, event_timestamp, or invalid speed)").alias("error_reason"),
                current_timestamp().cast("string").alias("timestamp"),
                col("raw_payload")
            ).alias("payload")
        ).selectExpr("to_json(payload) AS value")

        # Push to Kafka DLQ topic (downstream alerting)
        try:
            dlq_formatted.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_SERVER) \
                .option("topic", DLQ_TOPIC) \
                .save()
            logger.info(f"Batch {batch_id}: DLQ records published to Kafka topic '{DLQ_TOPIC}'.")
        except Exception as e:
            logger.error(f"Batch {batch_id}: Failed to publish DLQ to Kafka: {e}. Falling back to S3 only.")

        # Also persist DLQ records to S3 quarantine (long-term audit trail)
        try:
            malformed_df \
                .withColumn("error_reason", lit("Missing critical fields (VIN, event_timestamp, or invalid speed)")) \
                .withColumn("processed_at", current_timestamp()) \
                .write.mode("append").json(DLQ_QUARANTINE_PATH)
            logger.info(f"Batch {batch_id}: DLQ records persisted to S3 quarantine → {DLQ_QUARANTINE_PATH}")
        except Exception as e:
            logger.error(f"Batch {batch_id}: Failed to write DLQ to S3: {e}")

    # ── 4. Write valid records to Bronze S3 (append-only Parquet) ──
    if valid_count == 0:
        logger.info(f"Batch {batch_id}: No valid records to write. Done.")
        malformed_df.unpersist()
        valid_df.unpersist()
        return

    # Add timestamp columns for downstream processing & partitioning (date/hour)
    bronze_df = valid_df \
        .withColumn("event_ts", to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("event_unix", unix_timestamp(col("event_ts")).cast("double")) \
        .withColumn("date", to_date(col("event_ts"))) \
        .withColumn("hour", hour(col("event_ts"))) \
        .withColumn("bronze_ingested_at", current_timestamp()) \
        .select(
            "vin", "driver_id", "speed", "lat", "long",
            "event_timestamp", "event_ts", "event_unix",
            "bronze_ingested_at", "date", "hour"
        )

    # Atomic append to Bronze S3 — partitioned by date/hour
    try:
        bronze_df = bronze_df.repartition(4, "date", "hour")

        # Force execution to detect early failures
        record_count = bronze_df.count()
        logger.info(f"Batch {batch_id}: Records to write → {record_count}")

        bronze_df.write \
            .mode("append") \
            .option("compression", "snappy") \
            .partitionBy("date", "hour") \
            .parquet(BRONZE_TELEMETRY_PATH)

        logger.info(f"Batch {batch_id}: WRITE SUCCESS ✅")
    except Exception as e:
        logger.error(f"Batch {batch_id}: WRITE FAILED ❌ → {e}")

    # Release cached DataFrames
    malformed_df.unpersist()
    valid_df.unpersist()

# ──────────────────────────────────────────────────────────────
# BRONZE PIPELINE
# ──────────────────────────────────────────────────────────────
def run_bronze():
    logger.info("=" * 60)
    logger.info("Starting OmniRoute Bronze Streaming Layer (EC2 Spark)")
    logger.info("=" * 60)
    logger.info(f"  Kafka Server  : {KAFKA_SERVER}")
    logger.info(f"  Kafka Topic   : {KAFKA_TOPIC}")
    logger.info(f"  DLQ Topic     : {DLQ_TOPIC}")
    logger.info(f"  Bronze S3     : {BRONZE_TELEMETRY_PATH}")
    logger.info(f"  DLQ S3        : {DLQ_QUARANTINE_PATH}")
    logger.info(f"  Checkpoint    : {CHECKPOINT_PATH_BRONZE}")

    kafka_jar_version = "3.5.6" if _IS_EMR else "3.5.0"

    builder = (
        SparkSession.builder
        .appName("OmniRoute_Bronze_Streaming")
        .config("spark.jars.packages",
            f"org.apache.spark:spark-sql-kafka-0-10_2.12:{kafka_jar_version}")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.streaming.minBatchesToRetain", "2")
    )

    if _IS_EMR:
        logger.info("[ENV] Running on EMR — EMRFS handles s3:// natively.")
    else:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3.impl",  "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        )
        logger.info("[ENV] Running on EC2.")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created. Connecting to Kafka...")

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "5000") \
        .load()

    # ── Updated Checkpoint ──
    CHECKPOINT_V2 = CHECKPOINT_PATH_BRONZE + "_v2"

    query = raw_stream.writeStream \
        .foreachBatch(process_bronze_batch) \
        .option("checkpointLocation", CHECKPOINT_V2) \
        .trigger(processingTime="10 seconds") \
        .start()

    logger.info("Streaming query started. Listening for telemetry events. Press Ctrl+C to stop.")
    query.awaitTermination()

if __name__ == "__main__":
    run_bronze()
