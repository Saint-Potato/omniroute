"""
Bronze Ingestion — Unified Script
======================================
Reads fuel_transactions.csv, vehicle_assignment.csv, and vehicle_registry.csv 
from the S3 landing zone, validates them, and writes as Parquet to the ingested zone.

This script expects paths passed as arguments from Airflow variables.

Usage:
    spark-submit spark_jobs/batch/daily_ingest_bronze.py \
        --run-date 2026-04-16 \
        --landing-path s3a://path/landing/ \
        --ingested-path s3a://path/ingested/ \
        --quarantine-path s3a://path/quarantine/
"""

import os
import sys
import uuid
import argparse
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, to_date
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, LongType, IntegerType
)


# ──────────────────────────────────────────────
# Schema Definitions 
# ──────────────────────────────────────────────
SCHEMAS = {
    "fuel_transactions.csv": StructType([
        StructField("transaction_id", StringType(), False),
        StructField("vin", StringType(), False),
        StructField("fuel_liters", FloatType(), True),
        StructField("odometer_reading", FloatType(), True),
        StructField("timestamp", StringType(), True),
    ]),
    "vehicle_registry.csv": StructType([
        StructField("vin", StringType(), False),
        StructField("model", StringType(), True),
        StructField("mfg_year", IntegerType(), True),
        StructField("fuel_type", StringType(), True),
    ]),
    "vehicle_assignment.csv": StructType([
        StructField("vin", StringType(), False),
        StructField("driver_id", StringType(), False),
        StructField("start_timestamp", LongType(), True),
        StructField("end_timestamp", LongType(), True),
        StructField("daily_rate", FloatType(), True),
        StructField("region", StringType(), True),
    ])
}


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


def run(spark: SparkSession, run_date: str, landing_path: str, ingested_path: str, quarantine_path: str):
    """
    Process each file sequentially.
    """
    files_to_process = [
        "fuel_transactions.csv",
        "vehicle_registry.csv",
        "vehicle_assignment.csv"
    ]

    for source_filename in files_to_process:
        source = f"{landing_path}{source_filename}"
        
        # Ensure ingested path points to the specific dataset folder
        dataset_name = source_filename.replace('.csv', '')
        dest = f"{ingested_path}{dataset_name}"
        quarantine = f"{quarantine_path}dt={run_date}/{source_filename}"

        print(f"[{dataset_name}] Reading from: {source}")

        expected_schema = SCHEMAS[source_filename]
        
        try:
            df = (
                spark.read
                .option("header", "true")
                .schema(expected_schema)
                .csv(source)
            )

            # Validate
            validate_schema(df, [f.name for f in expected_schema.fields])
            row_count = df.count()

            if row_count == 0:
                raise ValueError("Empty file — 0 rows read")

            # Add metadata columns
            batch_id = str(uuid.uuid4())
            df = (df
                  .withColumn("load_date", to_date(lit(run_date)))
                  .withColumn("ingestion_timestamp", current_timestamp())
                  .withColumn("source_file_name", lit(source_filename))
                  .withColumn("batch_id", lit(batch_id)))

            # Write mode is append partitioned by load_date
            df.write.mode("append").partitionBy("load_date").parquet(dest)
            print(f"[{dataset_name}] ✓ Wrote {row_count} rows → {dest}/load_date={run_date}")

        except Exception as e:
            print(f"[{dataset_name}] ✗ Validation failed: {e}")
            print(f"[{dataset_name}] Moving to quarantine: {quarantine}")
            # Try to save raw without schema
            try:
                raw_df = spark.read.option("header", "true").csv(source)
                raw_df.write.mode("overwrite").parquet(quarantine)
            except Exception as read_err:
                print(f"[{dataset_name}] Error while moving to quarantine: {read_err}")
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default=str(date.today()),
                        help="Partition date (YYYY-MM-DD)")
    parser.add_argument("--landing-path", required=True,
                        help="S3 landing bucket path")
    parser.add_argument("--ingested-path", required=True,
                        help="S3 ingested bucket path")
    parser.add_argument("--quarantine-path", required=True,
                        help="S3 quarantine bucket path")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("OmniRoute_ingest_bronze")
        .getOrCreate()
    )

    # PySpark/Hadoop requires "s3a://" scheme instead of standard "s3://"
    def format_path(p):
        p = p.replace("s3://", "s3a://")
        return p if p.endswith("/") else p + "/"

    landing = format_path(args.landing_path)
    ingested = format_path(args.ingested_path)
    quarantine = format_path(args.quarantine_path)

    try:
        run(spark, args.run_date, landing, ingested, quarantine)
    finally:
        spark.stop()
