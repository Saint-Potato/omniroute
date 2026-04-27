"""
AWS Glue Job — Bronze Ingestion (Unified)
==========================================
Reads fuel_transactions.csv, vehicle_assignment.csv, and vehicle_registry.csv
from the S3 landing zone, validates schemas, adds audit metadata, and writes
as Parquet to the ingested zone. Malformed files are quarantined.

This script is designed to run as an AWS Glue ETL Job (Spark type).
All S3 paths are passed as Glue Job Parameters from the Airflow DAG,
which reads them from s3_paths.json.

Glue Job Parameters (passed via --arguments from Airflow):
    --run_date          : Partition date in YYYY-MM-DD format (e.g., 2026-04-16)
    --landing_path      : S3 path to the landing zone (e.g., s3://bucket/landing/)
    --ingested_path     : S3 path to the ingested zone (e.g., s3://bucket/ingested/)
    --quarantine_path   : S3 path to the quarantine zone (e.g., s3://bucket/quarantine/)

Note:
    - Glue uses "s3://" natively (not "s3a://"), unlike standalone Spark.
    - The script processes all 3 datasets sequentially within a single Glue job
      to minimize cold-start overhead.
"""

import sys
import uuid
from datetime import date
import boto3
from urllib.parse import urlparse

# ──────────────────────────────────────────────────────────────
# AWS Glue Imports
# ──────────────────────────────────────────────────────────────
# GlueContext wraps SparkContext with Glue-specific capabilities
# (e.g., DynamicFrames, job bookmarks, Data Catalog integration).
from awsglue.context import GlueContext

# Job is the entry point for every Glue ETL script —
# it handles initialization, commit, and bookmarking.
from awsglue.job import Job

# getResolvedOptions parses the Glue job parameters that Airflow
# passes via --arguments. These replace argparse in a Glue context.
from awsglue.utils import getResolvedOptions

# We still need SparkContext to bootstrap GlueContext.
from pyspark.context import SparkContext

# Standard PySpark imports for transformations and schema definitions.
from pyspark.sql.functions import lit, current_timestamp, to_date
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, LongType, IntegerType,
)


# ──────────────────────────────────────────────────────────────
# Schema Definitions
# ──────────────────────────────────────────────────────────────
# Each CSV file has a strict expected schema. Rows that don't
# conform will be caught during validation and quarantined.
SCHEMAS = {
    # fuel_transactions.csv — one row per fueling event
    "fuel_transactions.csv": StructType([
        StructField("transaction_id", StringType(), False),   # PK – unique txn ID
        StructField("vin", StringType(), False),              # FK – vehicle VIN
        StructField("fuel_liters", FloatType(), True),        # liters dispensed
        StructField("odometer_reading", FloatType(), True),   # odometer at fill-up
        StructField("timestamp", StringType(), True),         # ISO-8601 timestamp
    ]),

    # vehicle_registry.csv — master list of vehicles
    "vehicle_registry.csv": StructType([
        StructField("vin", StringType(), False),              # PK – vehicle VIN
        StructField("model", StringType(), True),             # vehicle model name
        StructField("mfg_year", IntegerType(), True),         # year of manufacture
        StructField("fuel_type", StringType(), True),         # DIESEL / PETROL / CNG
    ]),

    # vehicle_assignment.csv — driver ↔ vehicle assignments
    "vehicle_assignment.csv": StructType([
        StructField("vin", StringType(), False),              # FK – vehicle VIN
        StructField("driver_id", StringType(), False),        # FK – driver ID
        StructField("start_timestamp", LongType(), True),     # epoch start
        StructField("end_timestamp", LongType(), True),       # epoch end
        StructField("daily_rate", FloatType(), True),         # INR per day
        StructField("region", StringType(), True),            # operating region
    ]),
}


# ──────────────────────────────────────────────────────────────
# Validation Helper
# ──────────────────────────────────────────────────────────────
def validate_schema(df, expected_columns):
    """
    Verify that the DataFrame columns match the expected schema exactly.
    Raises ValueError if there are missing or unexpected columns —
    this signals a source-side contract violation.
    """
    actual_columns = set(df.columns)
    expected = set(expected_columns)
    if actual_columns != expected:
        missing = expected - actual_columns
        extra = actual_columns - expected
        raise ValueError(
            f"Schema mismatch — missing: {missing}, unexpected: {extra}"
        )


# ──────────────────────────────────────────────────────────────
# Path Normalization & S3 Move Helper
# ──────────────────────────────────────────────────────────────
def normalize_s3_path(path):
    """
    Ensure the S3 path ends with a trailing slash.
    Unlike standalone Spark (which needs s3a://), Glue uses s3:// natively,
    so no scheme conversion is needed here.
    """
    return path if path.endswith("/") else path + "/"

def move_s3_object(source_uri, target_uri):
    """
    Moves an S3 object from source_uri to target_uri by copying and deleting.
    Assumes "s3://bucket/key" format.
    """
    s3 = boto3.client('s3')
    src_parsed = urlparse(source_uri)
    tgt_parsed = urlparse(target_uri)
    
    src_bucket = src_parsed.netloc
    src_key = src_parsed.path.lstrip('/')
    tgt_bucket = tgt_parsed.netloc
    tgt_key = tgt_parsed.path.lstrip('/')
    
    copy_source = {
        'Bucket': src_bucket,
        'Key': src_key
    }
    
    print(f"    Moving {source_uri} -> {target_uri}")
    try:
        s3.copy_object(CopySource=copy_source, Bucket=tgt_bucket, Key=tgt_key)
        s3.delete_object(Bucket=src_bucket, Key=src_key)
    except Exception as e:
        print(f"    Failed to move S3 object: {e}")
        raise


# ──────────────────────────────────────────────────────────────
# Core Ingestion Logic
# ──────────────────────────────────────────────────────────────
def process_datasets(spark, run_date, landing_path, ingested_path, quarantine_path, archive_path):
    """
    Iterate over each CSV dataset, validate, enrich with metadata,
    and write as partitioned Parquet. Failed files go to quarantine.

    Args:
        spark           : Active SparkSession (from GlueContext)
        run_date        : Partition date string (YYYY-MM-DD)
        landing_path    : S3 path to raw CSV files
        ingested_path   : S3 path for validated Parquet output
        quarantine_path : S3 path for rejected / malformed files
        archive_path    : S3 path for archiving processed CSV files
    """
    # List of source CSV filenames to process in this job
    files_to_process = [
        "fuel_transactions.csv",
        "vehicle_registry.csv",
        "vehicle_assignment.csv",
    ]

    for source_filename in files_to_process:
        # Build full S3 source path to the CSV file
        source = f"{landing_path}{source_filename}"

        # Derive the dataset name (strip .csv extension) for output folder naming
        # e.g., "fuel_transactions.csv" → "fuel_transactions"
        dataset_name = source_filename.replace(".csv", "")

        # Build the ingested destination path
        # Parquet will be partitioned: {ingested_path}/{dataset_name}/load_date=YYYY-MM-DD/
        dest = f"{ingested_path}{dataset_name}"

        # Build the quarantine path for failed files
        # Quarantine is partitioned by date and preserves the source filename
        quarantine = f"{quarantine_path}dt={run_date}/{source_filename}"

        print(f"[{dataset_name}] Reading from: {source}")

        # Get the expected schema for this file
        expected_schema = SCHEMAS[source_filename]

        try:
            # ── Read CSV with enforced schema ──
            # header=true: first row is column names
            # schema: enforces data types (nulls on cast failure)
            df = (
                spark.read
                .option("header", "true")
                .schema(expected_schema)
                .csv(source)
            )

            # ── Validate column names match expected schema ──
            validate_schema(df, [f.name for f in expected_schema.fields])

            # ── Check for empty files ──
            row_count = df.count()
            if row_count == 0:
                raise ValueError("Empty file — 0 rows read")

            # ── Add audit/metadata columns ──
            # batch_id: UUID to uniquely identify this ingestion run
            # load_date: partition key — the logical execution date
            # ingestion_timestamp: wall-clock time when row was ingested
            # source_file_name: traceability back to the original CSV
            batch_id = str(uuid.uuid4())
            df = (
                df
                .withColumn("load_date", to_date(lit(run_date)))
                .withColumn("ingestion_timestamp", current_timestamp())
                .withColumn("source_file_name", lit(source_filename))
                .withColumn("batch_id", lit(batch_id))
            )

            # ── Write Parquet, partitioned by load_date ──
            # mode="append" allows multiple runs for different dates
            # to co-exist in the same dataset folder
            df.write.mode("append").partitionBy("load_date").parquet(dest)
            print(f"[{dataset_name}] ✓ Wrote {row_count} rows → {dest}/load_date={run_date}")
            
            # ── Move source file to archive ──
            archive_dest = f"{archive_path}dt={run_date}/{source_filename}"
            move_s3_object(source, archive_dest)

        except Exception as e:
            # ── Validation or read failed — quarantine the raw file ──
            print(f"[{dataset_name}] ✗ Validation failed: {e}")
            print(f"[{dataset_name}] Moving raw data to quarantine: {quarantine}")
            try:
                # Read without schema enforcement to preserve raw data
                raw_df = spark.read.option("header", "true").csv(source)
                raw_df.write.mode("overwrite").parquet(quarantine)
            except Exception as read_err:
                print(f"[{dataset_name}] Error while quarantining: {read_err}")
            # Re-raise so Glue marks this job run as FAILED
            raise


# ──────────────────────────────────────────────────────────────
# Glue Job Entry Point
# ──────────────────────────────────────────────────────────────
# This block runs when the Glue job starts.
# It replaces the __main__ + argparse pattern from standalone Spark.

# Step 1: Parse Glue job parameters
# getResolvedOptions reads --JOB_NAME plus any custom args passed
# via the Airflow GlueJobOperator's script_args parameter.
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",          # Glue-managed: name of this job
    "run_date",          # Custom: partition date (YYYY-MM-DD)
    "landing_path",      # Custom: S3 landing zone path
    "ingested_path",     # Custom: S3 ingested zone path
    "quarantine_path",   # Custom: S3 quarantine zone path
    "archive_path",      # Custom: S3 archive zone path
])

# Step 2: Initialize Spark + Glue contexts
# SparkContext is the low-level Spark entry point.
# GlueContext wraps it with Glue features (DynamicFrames, bookmarks, etc.)
sc = SparkContext()
glue_context = GlueContext(sc)

# Get the SparkSession from GlueContext — this is what we pass to our
# processing function (identical to SparkSession.builder.getOrCreate())
spark = glue_context.spark_session

# Step 3: Initialize the Glue Job object
# This enables job bookmarking and tracks job lifecycle.
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# Step 4: Normalize S3 paths (ensure trailing slashes)
landing = normalize_s3_path(args["landing_path"])
ingested = normalize_s3_path(args["ingested_path"])
quarantine = normalize_s3_path(args["quarantine_path"])
archive = normalize_s3_path(args["archive_path"])

# Step 5: Set the run date — use provided date or default to today
run_date = args.get("run_date", str(date.today()))

print("=" * 60)
print(f"  OmniRoute Bronze Ingestion — Glue Job")
print(f"  Run Date   : {run_date}")
print(f"  Landing    : {landing}")
print(f"  Ingested   : {ingested}")
print(f"  Quarantine : {quarantine}")
print(f"  Archive    : {archive}")
print("=" * 60)

# Step 6: Execute the ingestion pipeline
try:
    process_datasets(spark, run_date, landing, ingested, quarantine, archive)
    print("✓ All datasets ingested successfully.")
except Exception as e:
    print(f"✗ Bronze ingestion failed: {e}")
    raise  # Let Glue mark the job as FAILED
finally:
    # Step 7: Commit the Glue job
    # job.commit() is REQUIRED — it finalizes bookmarks and signals
    # to Glue that the job completed. Without this, Glue won't track
    # which data has already been processed.
    job.commit()
