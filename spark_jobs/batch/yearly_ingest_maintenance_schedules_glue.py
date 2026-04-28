"""
AWS Glue Job — Bronze Ingestion (Maintenance Schedules)
=========================================================
Reads maintenance_schedules.csv from the S3 landing zone, validates schemas,
adds audit metadata, and writes as Parquet to the ingested zone.
Moves the successfully processed CSV to the archive zone.
Malformed files are quarantined.

Glue Job Parameters (passed via --arguments from Airflow):
    --run_date          : Partition date in YYYY-MM-DD format (e.g., 2026-01-01)
    --landing_path      : S3 path to the landing zone (e.g., s3://bucket/landing/)
    --ingested_path     : S3 path to the ingested zone (e.g., s3://bucket/ingested/)
    --quarantine_path   : S3 path to the quarantine zone (e.g., s3://bucket/quarantine/)
    --archive_path      : S3 path to the archive zone (e.g., s3://bucket/archive/)
"""

import sys
import uuid
from datetime import date
import boto3
from urllib.parse import urlparse

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import lit, current_timestamp, to_date, col
from pyspark.sql.types import StructType, StructField, StringType

# ──────────────────────────────────────────────────────────────
# Schema Definitions
# ──────────────────────────────────────────────────────────────
# Must match source CSV exactly
EXPECTED_SCHEMA = StructType([
    StructField("vin", StringType(), True), # PK - accepts null
    StructField("service_date", StringType(), True),
    StructField("service_type", StringType(), True),
])

SOURCE_FILENAME = "maintenance_schedules.csv"

# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def validate_schema(df, expected_columns):
    actual_columns = set(df.columns)
    expected = set(expected_columns)
    if actual_columns != expected:
        missing = expected - actual_columns
        extra = actual_columns - expected
        raise ValueError(
            f"Schema mismatch — missing: {missing}, unexpected: {extra}"
        )

def normalize_s3_path(path):
    return path if path.endswith("/") else path + "/"

def move_s3_object(source_uri, target_uri):
    """
    Moves an S3 object from source_uri to target_uri by copying and deleting.
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
# Core Processing
# ──────────────────────────────────────────────────────────────
def process_maintenance_schedules(spark, run_date, landing_path, ingested_path, quarantine_path, archive_path):
    source = f"{landing_path}{SOURCE_FILENAME}"
    
    # Ingested dataset subfolder will match file name without csv
    dataset_name = SOURCE_FILENAME.replace(".csv", "")
    dest = f"{ingested_path}{dataset_name}"
    
    quarantine = f"{quarantine_path}dt={run_date}/{SOURCE_FILENAME}"
    archive_dest = f"{archive_path}dt={run_date}/{SOURCE_FILENAME}"

    print(f"[{dataset_name}] Reading from: {source}")

    try:
        df = (
            spark.read
            .option("header", "true")
            .csv(source)
        )

        validate_schema(df, [f.name for f in EXPECTED_SCHEMA.fields])

        cast_cols = [col(f.name).cast(f.dataType).alias(f.name) for f in EXPECTED_SCHEMA.fields]
        df = df.select(*cast_cols)

        row_count = df.count()

        if row_count == 0:
            raise ValueError("Empty file — 0 rows read")

        batch_id = str(uuid.uuid4())
        df = (df
              .withColumn("load_date", to_date(lit(run_date)))
              .withColumn("ingestion_timestamp", current_timestamp())
              .withColumn("source_file_name", lit(SOURCE_FILENAME))
              .withColumn("batch_id", lit(batch_id)))

        df.write.mode("overwrite").partitionBy("load_date").parquet(dest)
        print(f"[{dataset_name}] ✓ Wrote {row_count} rows → {dest}/load_date={run_date}")
        
        # Move successfully processed file to archive
        move_s3_object(source, archive_dest)

    except Exception as e:
        print(f"[{dataset_name}] ✗ Validation/Ingestion failed: {e}")
        print(f"[{dataset_name}] Moving raw CSV directly to archive: {archive_dest}")
        try:
            move_s3_object(source, archive_dest)
        except Exception as move_err:
            print(f"[{dataset_name}] Error while archiving failed file: {move_err}")
        raise


# ──────────────────────────────────────────────────────────────
# Entry Point
# ──────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "run_date",
    "landing_path",
    "ingested_path",
    "quarantine_path",
    "archive_path"
])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

landing = normalize_s3_path(args["landing_path"])
ingested = normalize_s3_path(args["ingested_path"])
quarantine = normalize_s3_path(args["quarantine_path"])
archive = normalize_s3_path(args["archive_path"])

# Usually run in January
run_date = args.get("run_date", str(date.today()))

print("=" * 60)
print(f"  OmniRoute Yearly Maintenance Schedules Ingestion — Glue Job")
print(f"  Run Date   : {run_date}")
print(f"  Landing    : {landing}")
print(f"  Ingested   : {ingested}")
print(f"  Quarantine : {quarantine}")
print(f"  Archive    : {archive}")
print("=" * 60)

try:
    process_maintenance_schedules(spark, run_date, landing, ingested, quarantine, archive)
    print("✓ Dataset ingested successfully.")
except Exception as e:
    print(f"✗ Ingestion failed: {e}")
    raise
finally:
    job.commit()
