import os
import sys

# Add parent dir to sys.path directly to ensure imports work correctly
CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
PARENT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from config import TELEMETRY_CONFIG

# ================================
# CONFIGURATION
# ================================
KAFKA_SERVER = TELEMETRY_CONFIG["KAFKA_SERVER"]
KAFKA_TOPIC = TELEMETRY_CONFIG["KAFKA_TOPIC"]

def create_spark_session():
    """Create and return a configured SparkSession."""
    print("Initializing SparkSession...")
    return SparkSession.builder \
        .appName("OmniRoute_Telemetry_Streaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

# ================================
# MAIN STREAMING FUNCTION
# ================================
def run_streaming():
    spark = create_spark_session()
    
    # Hide overly verbose logs
    spark.sparkContext.setLogLevel("WARN")
    print(f"Connecting to Kafka topic '{KAFKA_TOPIC}' at {KAFKA_SERVER}...")

    # Define the exact JSON schema that the telemetry producer emits
    telemetry_schema = StructType([
        StructField("vin", StringType(), True),
        StructField("driver_id", StringType(), True),
        StructField("speed", IntegerType(), True),
        StructField("lat", DoubleType(), True),
        StructField("long", DoubleType(), True),
        StructField("event_timestamp", StringType(), True)
    ])

    # 1. Read Stream from Kafka
    raw_kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parse the Kafka binary value as String, then to JSON using schema
    parsed_df = raw_kafka_df \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), telemetry_schema).alias("data")) \
        .select("data.*")

    # 3. Write Stream output to the Console
    # Use trigger to print batches every 5 seconds to reduce console jitter
    print("Starting streaming query to console. Press Ctrl+C to exit.\n")
    query = parsed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .trigger(processingTime="5 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_streaming()
