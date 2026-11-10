"""
===========================================================
OmniRoute Smart Logistics Engine — Bronze Streaming Layer (AWS Glue)
===========================================================

Glue port of bronze_streaming.py (the EMR/EC2 version). Same Bronze logic,
same output paths and schema, so silver_streaming.py reads it unchanged:

  1. Read raw telemetry JSON from Kafka
  2. Validate schema (DLQ any malformed records)
  3. Append valid raw events to S3 Bronze (date/hour-partitioned Parquet)
  4. Send DLQ records to the Kafka DLQ topic AND S3 quarantine

What changed for Glue:
  - Paths and Kafka settings come from job parameters instead of
    s3_paths.json on S3 and environment variables.
  - The Kafka connector comes from --extra-jars (Glue can't download
    spark.jars.packages from Maven at run time).
  - --trigger_mode available_now (default) reads everything new since the
    last checkpoint, writes it and stops, so the job can be run on demand
    like the batch jobs. --trigger_mode continuous runs micro-batches every
    --trigger_interval_seconds until the job is stopped or times out, like
    the EMR version.

Job type: Spark (glueetl), Glue 5.0.

Glue job parameters:
  Required
    --kafka_bootstrap_servers   host:port[,host:port] of the brokers (e.g. 172.31.65.131:9092)
    --bronze_telemetry_path     s3://.../ingested/telemetry
    --dlq_quarantine_path       s3://.../quarantine/telemetry_dlq
    --checkpoint_path           s3://.../checkpoints/bronze_telemetry_glue
  Optional
    --kafka_topic               default vehicle_telemetry_topic
    --dlq_topic                 default omniroute-telemetry-dlq
    --starting_offsets          default earliest (used only when the checkpoint is empty)
    --max_offsets_per_trigger   default 5000
    --trigger_mode              available_now (default) | continuous
    --trigger_interval_seconds  default 10 (continuous mode only)
"""
import sys
import logging

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
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

REQUIRED_PARAMS = [
    "JOB_NAME",
    "kafka_bootstrap_servers",
    "bronze_telemetry_path",
    "dlq_quarantine_path",
    "checkpoint_path",
]
OPTIONAL_PARAMS = {
    "kafka_topic": "vehicle_telemetry_topic",
    "dlq_topic": "omniroute-telemetry-dlq",
    "starting_offsets": "earliest",
    "max_offsets_per_trigger": "5000",
    "trigger_mode": "available_now",
    "trigger_interval_seconds": "10",
}

# getResolvedOptions fails on any listed name that wasn't passed, so only
# ask for the optional ones that are present.
_passed = [name for name in OPTIONAL_PARAMS if f"--{name}" in sys.argv]
args = {**OPTIONAL_PARAMS, **getResolvedOptions(sys.argv, REQUIRED_PARAMS + _passed)}

KAFKA_SERVER = args["kafka_bootstrap_servers"]
KAFKA_TOPIC  = args["kafka_topic"]
DLQ_TOPIC    = args["dlq_topic"]

BRONZE_TELEMETRY_PATH  = args["bronze_telemetry_path"].rstrip("/")
DLQ_QUARANTINE_PATH    = args["dlq_quarantine_path"].rstrip("/")
CHECKPOINT_PATH_BRONZE = args["checkpoint_path"].rstrip("/")

TRIGGER_MODE = args["trigger_mode"].strip().lower()
if TRIGGER_MODE not in ("available_now", "continuous"):
    logger.critical(f"--trigger_mode must be available_now or continuous, got '{TRIGGER_MODE}'")
    sys.exit(1)

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
# MICRO-BATCH PROCESSOR (unchanged from the EMR version)
# ──────────────────────────────────────────────────────────────
def process_bronze_batch(batch_df: DataFrame, batch_id: int):
    """
    Called by Spark Structured Streaming for each micro-batch.
    Pure Bronze logic: parse → validate → DLQ → write raw Parquet to S3.
    """
    if batch_df.isEmpty():
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

    # Append to Bronze S3 — partitioned by date/hour
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
        # Kept from the EMR version: a failed write is logged and the batch
        # still counts as processed, so its events are not re-read.
        logger.error(f"Batch {batch_id}: WRITE FAILED ❌ → {e}")

    # Release cached DataFrames
    malformed_df.unpersist()
    valid_df.unpersist()

# ──────────────────────────────────────────────────────────────
# BRONZE PIPELINE
# ──────────────────────────────────────────────────────────────
def run_bronze(spark):
    logger.info("=" * 60)
    logger.info("Starting OmniRoute Bronze Streaming Layer (AWS Glue)")
    logger.info("=" * 60)
    logger.info(f"  Kafka Server  : {KAFKA_SERVER}")
    logger.info(f"  Kafka Topic   : {KAFKA_TOPIC}")
    logger.info(f"  DLQ Topic     : {DLQ_TOPIC}")
    logger.info(f"  Bronze S3     : {BRONZE_TELEMETRY_PATH}")
    logger.info(f"  DLQ S3        : {DLQ_QUARANTINE_PATH}")
    logger.info(f"  Checkpoint    : {CHECKPOINT_PATH_BRONZE}")
    logger.info(f"  Trigger       : {TRIGGER_MODE}")

    spark.conf.set("spark.sql.shuffle.partitions", "50")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.streaming.minBatchesToRetain", "2")

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", args["starting_offsets"]) \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", args["max_offsets_per_trigger"]) \
        .load()

    writer = raw_stream.writeStream \
        .foreachBatch(process_bronze_batch) \
        .option("checkpointLocation", CHECKPOINT_PATH_BRONZE)

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
        run_bronze(spark)
    finally:
        job.commit()
