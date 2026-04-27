import os
import sys
import json
import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, to_date, lit, year, month, dayofmonth, current_timestamp, struct
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# ================================================================
# CONFIGURATION AND PATHS
# ================================================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))

# Load paths from s3_paths.json
S3_PATHS_FILE = os.path.join(PROJECT_ROOT, "s3_paths.json")
try:
    with open(S3_PATHS_FILE, "r") as f:
        S3_CONFIG = json.load(f)
except Exception as e:
    print(f"CRITICAL ERROR: Failed to load {S3_PATHS_FILE}: {e}")
    sys.exit(1)

BRONZE_INGESTED_PATH = os.path.join(S3_CONFIG["bronze"]["ingested"], "telemetry")
BRONZE_DLQ_PATH = os.path.join(S3_CONFIG["bronze"]["quarantine"], "telemetry_dlq")
CHECKPOINT_LOCATION = os.path.join(S3_CONFIG["bronze"]["base_bucket"], S3_CONFIG["bronze"]["base_prefix"], "checkpoints", "telemetry")

# Kafka Settings
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
KAFKA_TOPIC = "vehicle_telemetry_topic"
DLQ_TOPIC = "omniroute-telemetry-dlq"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("TelemetryBronzeIngest")

TELEMETRY_SCHEMA = StructType([
    StructField("vin", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("speed", IntegerType(), True),
    StructField("lat", DoubleType(), True),
    StructField("long", DoubleType(), True),
    StructField("event_timestamp", StringType(), True),
])

# ================================================================
# MICRO-BATCH PROCESSOR
# ================================================================

def process_bronze_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    logger.info(f"Processing Bronze Micro-Batch {batch_id}...")

    # 1. Parse JSON safely
    parsed_df = batch_df.select(
        col("value").cast("string").alias("raw_payload"),
        from_json(col("value").cast("string"), TELEMETRY_SCHEMA).alias("data")
    )

    # 2. DLQ Separation
    # A record is malformed if VIN is missing or event_timestamp is missing
    malformed_df = parsed_df.filter(
        col("data.vin").isNull() | (col("data.vin") == "") |
        col("data.event_timestamp").isNull() | (col("data.event_timestamp") == "")
    )

    valid_df = parsed_df.filter(
        col("data.vin").isNotNull() & (col("data.vin") != "") &
        col("data.event_timestamp").isNotNull() & (col("data.event_timestamp") != "")
    ).select("data.*")

    # 3. Handle DLQ Records (Write to Kafka DLQ Topic & S3 Quarantine)
    dlq_count = malformed_df.count()
    if dlq_count > 0:
        logger.warning(f"Batch {batch_id}: Found {dlq_count} malformed records. Sending to DLQ...")
        
        # Format DLQ payload
        dlq_formatted = malformed_df.select(
            struct(
                lit("Missing critical fields (VIN or event_timestamp)").alias("error_reason"),
                current_timestamp().cast("string").alias("timestamp"),
                col("raw_payload")
            ).alias("payload")
        ).selectExpr("to_json(payload) AS value")
        
        # Push to Kafka DLQ Topic
        try:
            dlq_formatted.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_SERVER) \
                .option("topic", DLQ_TOPIC) \
                .save()
        except Exception as e:
            logger.error(f"Failed to publish to Kafka DLQ: {e}")

        # Also persist to S3 Quarantine for batch review
        malformed_df.withColumn("error_reason", lit("Missing critical fields")) \
            .withColumn("processed_at", current_timestamp()) \
            .write.mode("append").json(BRONZE_DLQ_PATH)

    # 4. Process Valid Records -> Bronze S3
    if valid_df.rdd.isEmpty():
        return
        
    valid_count = valid_df.count()
    
    # Cast timestamp and create partition columns
    bronze_df = valid_df.withColumn(
        "event_ts", to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss")
    ).withColumn("year", year(col("event_ts"))) \
     .withColumn("month", month(col("event_ts"))) \
     .withColumn("day", dayofmonth(col("event_ts")))

    # Deduplicate within batch to prevent double-writes exactly at this layer
    bronze_df = bronze_df.dropDuplicates(["vin", "event_timestamp"])

    # Write append-only to Bronze S3 Partitioned by Date
    bronze_df.write \
        .mode("append") \
        .partitionBy("year", "month", "day") \
        .parquet(BRONZE_INGESTED_PATH)
        
    logger.info(f"Batch {batch_id}: Successfully ingested {valid_count} valid records to Bronze.")

# ================================================================
# MAIN STREAMING ENTRY
# ================================================================

def run_bronze_ingestion():
    logger.info("Starting OmniRoute Bronze Telemetry Ingestion (EC2 Spark Streaming)")

    spark = SparkSession.builder \
        .appName("OmniRoute_Bronze_Ingestion") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    query = raw_stream.writeStream \
        .foreachBatch(process_bronze_batch) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_bronze_ingestion()
