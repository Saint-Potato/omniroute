import sys
import json
import os
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, broadcast, when, lit, to_date
)

# Initialize Glue Job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_PATHS_JSON'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ================================================================
# CONFIGURATION & PATHS
# ================================================================

# In production Glue, we pass the S3 path to the config JSON as an argument.
# For local testing/development, fallback to a local path if needed.
s3_paths_uri = args.get('S3_PATHS_JSON', 's3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/scripts/s3_paths.json')

# In PySpark on Glue, reading a JSON config from S3 can be done via Spark Context:
config_df = spark.read.json(s3_paths_uri, multiLine=True)
s3_config = config_df.collect()[0].asDict(recursive=True)

BRONZE_TELEMETRY = os.path.join(s3_config["bronze"]["ingested"], "telemetry")
SILVER_TELEMETRY = s3_config["silver"]["tables"]["telemetry"]
ZONES_FILE = os.path.join(s3_config["bronze"]["landing"], "restricted_zones.json")
ASSIGNMENTS_FILE = os.path.join(s3_config["bronze"]["landing"], "vehicle_assignment.csv")

SPEED_THRESHOLD = 110

# ================================================================
# 1. READ BRONZE TELEMETRY (PREDICATE PUSHDOWN OPTIMIZATION)
# ================================================================
# We only want to process recently ingested data. Assuming a daily run:
# Using Glue's DynamicFrame for pushdown (or spark.read with partitioning)
# Here we use native Spark for complex joins, but we can filter by partition.
print(f"Reading raw telemetry from {BRONZE_TELEMETRY}")

raw_df = spark.read.parquet(BRONZE_TELEMETRY)

# Deduplicate
deduped_df = raw_df.dropDuplicates(["vin", "event_timestamp"])

# ================================================================
# 2. LOAD REFERENCE DATA
# ================================================================

# Load Zones
try:
    zones_df = spark.read.json(ZONES_FILE, multiLine=True)
    zones_df = zones_df.select("zone_name", "min_lat", "max_lat", "min_long", "max_long")
except Exception as e:
    print(f"Warning: Zones file not found or malformed at {ZONES_FILE}. Using empty df. Error: {e}")
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    zones_schema = StructType([
        StructField("zone_name", StringType()),
        StructField("min_lat", DoubleType()), StructField("max_lat", DoubleType()),
        StructField("min_long", DoubleType()), StructField("max_long", DoubleType())
    ])
    zones_df = spark.createDataFrame([], zones_schema)

# Load Assignments (Assume we use the landing path directly or from Silver)
try:
    assignments_df = spark.read.option("header", "true").csv(ASSIGNMENTS_FILE)
    # Simplify for stream: just get the active driver (end_timestamp is null)
    active_assignments = assignments_df.filter(
        (col("end_timestamp").isNull()) | (col("end_timestamp") == "")
    ).select(col("vin").alias("assign_vin"), col("driver_id").alias("assign_driver_id"))
except Exception as e:
    print(f"Warning: Assignments file not found at {ASSIGNMENTS_FILE}. Error: {e}")
    from pyspark.sql.types import StructType, StructField, StringType
    assign_schema = StructType([StructField("assign_vin", StringType()), StructField("assign_driver_id", StringType())])
    active_assignments = spark.createDataFrame([], assign_schema)

# ================================================================
# 3. ENRICHMENT & BUSINESS LOGIC
# ================================================================

# Driver Resolution
enriched_df = deduped_df.join(
    broadcast(active_assignments),
    deduped_df.vin == active_assignments.assign_vin,
    how="left"
).drop("assign_vin")

enriched_df = enriched_df.withColumn(
    "resolved_driver_id",
    when(
        (col("driver_id").isNull()) | (col("driver_id") == "") | (col("driver_id") == "DRV_UNKNOWN"),
        col("assign_driver_id")
    ).otherwise(col("driver_id"))
)

# Speed Violations
enriched_df = enriched_df.withColumn(
    "is_speeding",
    when(col("speed") > SPEED_THRESHOLD, lit(True)).otherwise(lit(False))
)

# Geofence Violations
if zones_df.count() > 0:
    geo_df = enriched_df.crossJoin(broadcast(zones_df)).filter(
        (col("lat").between(col("min_lat"), col("max_lat"))) &
        (col("long").between(col("min_long"), col("max_long")))
    ).select(
        col("vin"),
        col("event_timestamp"),
        col("zone_name").alias("matched_zone_name"),
    ).dropDuplicates(["vin", "event_timestamp"])

    enriched_df = enriched_df.join(geo_df, on=["vin", "event_timestamp"], how="left")
    enriched_df = enriched_df.withColumn("is_in_zone", col("matched_zone_name").isNotNull())
else:
    enriched_df = enriched_df.withColumn("matched_zone_name", lit(None).cast("string"))
    enriched_df = enriched_df.withColumn("is_in_zone", lit(False))

# Violation Aggregation
enriched_df = enriched_df.withColumn("is_violation", col("is_speeding") | col("is_in_zone"))
enriched_df = enriched_df.withColumn(
    "violation_type",
    when(col("is_speeding") & col("is_in_zone"), lit("SPEED_VIOLATION|ZONE_INTRUSION"))
    .when(col("is_speeding"), lit("SPEED_VIOLATION"))
    .when(col("is_in_zone"), lit("ZONE_INTRUSION"))
    .otherwise(lit(None))
)

# Format for Silver
silver_df = enriched_df.select(
    col("vin"),
    col("resolved_driver_id").alias("driver_id"),
    col("speed"),
    col("lat"),
    col("long"),
    col("event_timestamp"),
    col("is_violation"),
    col("violation_type"),
    col("matched_zone_name").alias("zone_name"),
    col("year"),
    col("month"),
    col("day")
)

# ================================================================
# 4. WRITE TO SILVER S3 (DELTA LAKE OR PARQUET)
# ================================================================
print(f"Writing enriched telemetry to {SILVER_TELEMETRY}")

silver_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(SILVER_TELEMETRY)

job.commit()
