"""
Bronze Ingestion — Vehicle Registry
=====================================
Reads vehicle_registry.csv from the S3 landing zone, validates the file,
and writes it as Parquet to the ingested zone.

This is a daily FULL SNAPSHOT — appends to the partition each run.

Usage:
    spark-submit spark_jobs/batch/ingest_vehicle_registry.py --run-date 2026-04-16
"""

import sys
import uuid
import argparse
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# ──────────────────────────────────────────────
# Schema definition (must match source CSV exactly)
# ──────────────────────────────────────────────
EXPECTED_SCHEMA = StructType([
    StructField("vin", StringType(), False),
    StructField("model", StringType(), True),
    StructField("mfg_year", IntegerType(), True),
    StructField("fuel_type", StringType(), True),
])

SOURCE_FILENAME = "vehicle_registry.csv"

# S3 paths — override via env vars in production
LANDING_PATH = "s3a://omniroute-bronze/landing"
INGESTED_PATH = "s3a://omniroute-bronze/ingested/vehicle_registry"
QUARANTINE_PATH = "s3a://omniroute-bronze/quarantine"


def validate_schema(df, expected_columns):
    """Check that the DataFrame columns match the expected set."""
    actual_columns = set(df.columns)
    expected = set(expected_columns)
    if actual_columns != expected:
        missing = expected - actual_columns
        extra = actual_columns - expected
        raise ValueError(
            f"Schema mismatch — missing: {missing}, unexpected: {extra}"
        )


def run(spark: SparkSession, run_date: str):
    """
    1. Read CSV from landing/
    2. Validate schema
    3. Write Parquet to ingested/ (overwrite partition)
    4. Delete source CSV from landing/
    On validation failure → move file to quarantine/
    """
    source = f"{LANDING_PATH}/{SOURCE_FILENAME}"
    quarantine = f"{QUARANTINE_PATH}/dt={run_date}/{SOURCE_FILENAME}"

    print(f"[vehicle_registry] Reading from: {source}")

    try:
        df = (
            spark.read
            .option("header", "true")
            .schema(EXPECTED_SCHEMA)
            .csv(source)
        )

        # Validate
        validate_schema(df, [f.name for f in EXPECTED_SCHEMA.fields])
        row_count = df.count()

        if row_count == 0:
            raise ValueError("Empty file — 0 rows read")

        # Add metadata columns
        batch_id = str(uuid.uuid4())
        df = (df
              .withColumn("load_date", to_date(lit(run_date)))
              .withColumn("ingestion_timestamp", current_timestamp())
              .withColumn("source_file_name", lit(SOURCE_FILENAME))
              .withColumn("batch_id", lit(batch_id)))

        # Write (append mode), partitioned by load_date
        df.write.mode("append").partitionBy("load_date").parquet(INGESTED_PATH)
        print(f"[vehicle_registry] ✓ Wrote {row_count} rows → {INGESTED_PATH}/load_date={run_date}")

    except Exception as e:
        print(f"[vehicle_registry] ✗ Validation failed: {e}")
        print(f"[vehicle_registry] Moving to quarantine: {quarantine}")
        # Read raw file and dump to quarantine as-is
        raw_df = spark.read.option("header", "true").csv(source)
        raw_df.write.mode("overwrite").parquet(quarantine)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default=str(date.today()),
                        help="Partition date (YYYY-MM-DD)")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("OmniRoute_ingest_vehicle_registry")
        .getOrCreate()
    )

    try:
        run(spark, args.run_date)
    finally:
        spark.stop()
