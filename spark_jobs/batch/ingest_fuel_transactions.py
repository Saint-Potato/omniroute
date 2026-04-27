"""
Bronze Ingestion — Fuel Transactions
======================================
Reads fuel_transactions.csv from the S3 landing zone, performs advanced data quality 
validation, and writes it to the ingested zone as an optimized Parquet table.

Features:
- Parquet format with dynamic partition overwrite for idempotency.
- Column header pre-validation to avoid false-positive schema matches.
- Row-level Data Quality rules (splits valid vs invalid records).
- Robust Python logging.
- DQ metrics emission for observability.
- Environment-configurable S3 paths.

Usage:
    spark-submit spark_jobs/batch/ingest_fuel_transactions.py --run-date 2026-04-16
"""

import os
import uuid
import json
import argparse
import logging
from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, to_date, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType




# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ingest_fuel_transactions")

# Schema Definition
EXPECTED_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("vin", StringType(), False),
    StructField("fuel_liters", FloatType(), True),
    StructField("odometer_reading", FloatType(), True),
    StructField("timestamp", StringType(), True),
])

SOURCE_FILENAME = "fuel_transactions.csv"

# S3 paths — configurable via environment variables
LANDING_PATH = os.environ.get("LANDING_PATH", "s3a://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/landing")
INGESTED_PATH = os.environ.get("INGESTED_PATH", "s3a://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/ingested/") + "fuel_transactions"
QUARANTINE_PATH = os.environ.get("QUARANTINE_PATH", "s3a://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/quarantine")
METRICS_PATH = os.environ.get("METRICS_PATH", "s3a://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/metrics")


def run(spark: SparkSession, run_date: str):
    source = f"{LANDING_PATH}/{SOURCE_FILENAME}"
    logger.info(f"Starting ingestion for {run_date}. Reading from: {source}")

    # 1. Pre-validate File Columns (to catch completely wrong files before enforcing schema)
    try:
        raw_df = spark.read.option("header", "true").csv(source)
        actual_columns = set(raw_df.columns)
        expected_columns = set([f.name for f in EXPECTED_SCHEMA.fields])
        
        if actual_columns != expected_columns:
            missing = expected_columns - actual_columns
            extra = actual_columns - expected_columns
            logger.error(f"Schema mismatch detected! Missing: {missing}, Extra: {extra}")
            raise ValueError(f"Schema mismatch. Missing: {missing}, Extra: {extra}")
            
    except Exception as e:
        logger.error(f"Failed to read/validate source. Moving raw text to quarantine. Error: {e}")
        try:
            quarantine = f"{QUARANTINE_PATH}/raw_files/dt={run_date}/{SOURCE_FILENAME}_{uuid.uuid4()}"
            spark.read.text(source).write.mode("append").text(quarantine)
        except Exception as inner_e:
            logger.error(f"Failed to write raw text to quarantine: {inner_e}")
        raise

    # 2. Read with Enforced Schema
    df = spark.read.option("header", "true").schema(EXPECTED_SCHEMA).csv(source)
    row_count = df.count()
    
    if row_count == 0:
        logger.warning("Empty file — 0 rows read. Exiting gracefully.")
        return

    # 3. Add Metadata & Parse Strings
    batch_id = str(uuid.uuid4())
    df = (df
          .withColumn("load_date", to_date(lit(run_date)))
          .withColumn("ingestion_timestamp", current_timestamp())
          .withColumn("source_file_name", lit(SOURCE_FILENAME))
          .withColumn("batch_id", lit(batch_id))
          .withColumn("timestamp_parsed", to_timestamp(col("timestamp"))))

    # 4. Data Quality Rules
    # Only keep logically valid records
    valid_df = df.filter(
        (col("fuel_liters") > 0) & 
        (col("odometer_reading") >= 0) & 
        col("vin").isNotNull()
    )
    invalid_df = df.subtract(valid_df)

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    logger.info(f"Data Quality Scan: {valid_count} valid rows | {invalid_count} invalid rows.")

    # 5. Row count assertion — alert on high failure rate
    if row_count > 0 and invalid_count / row_count > 0.20:
        logger.critical(
            f"DATA QUALITY ALARM: {invalid_count}/{row_count} rows "
            f"({invalid_count / row_count * 100:.1f}%) failed validation!"
        )

    # 6. Write Valid Data to Parquet (Idempotent using dynamic partition overwrite)
    if valid_count > 0:
        logger.info(f"Writing {valid_count} valid records to Parquet: {INGESTED_PATH}")
        (valid_df.write
         .mode("overwrite")
         .partitionBy("load_date")
         .parquet(INGESTED_PATH))

    # 7. Quarantine Invalid Rows (Parquet format, append)
    if invalid_count > 0:
        quarantine_parquet = f"{QUARANTINE_PATH}/fuel_transactions"
        logger.warning(f"Quarantining {invalid_count} bad rows to Parquet: {quarantine_parquet}")
        (invalid_df.write
         .mode("append")
         .partitionBy("load_date")
         .parquet(quarantine_parquet))

    # 8. Emit DQ Metrics
    job_name = "ingest_fuel_transactions"
    dq_pass_rate = round(valid_count / row_count * 100, 2) if row_count > 0 else 0
    metrics = {
        "job_name": job_name,
        "run_date": run_date,
        "batch_id": batch_id,
        "total_rows": row_count,
        "valid_rows": valid_count,
        "invalid_rows": invalid_count,
        "quarantined_rows": invalid_count,
        "dq_pass_rate": dq_pass_rate,
        "timestamp": datetime.now().isoformat(),
    }
    try:
        metrics_output = f"{METRICS_PATH}/dt={run_date}/{job_name}"
        spark.sparkContext.parallelize([json.dumps(metrics)]).coalesce(1).saveAsTextFile(metrics_output)
        logger.info(f"DQ metrics written to: {metrics_output}")
    except Exception as e:
        logger.warning(f"Failed to write DQ metrics (non-fatal): {e}")

    logger.info("Ingestion completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default=str(date.today()), help="Partition date (YYYY-MM-DD)")
    args = parser.parse_args()

    # Configure SparkSession with production tuning
    spark = (
        SparkSession.builder
        .appName("OmniRoute_ingest_fuel_transactions")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )

    try:
        run(spark, args.run_date)
    finally:
        spark.stop()
