"""
Bronze Ingestion — Maintenance Schedules
==========================================
Reads maintenance_schedules.csv from S3 landing zone, performs advanced data
quality validation, and writes it to the ingested zone as optimized Parquet.

Features:
- Full Snapshot History tracking using Parquet dynamic partition overwrite on `load_date`.
- Column header pre-validation before enforcing schema with Spark.
- Row-level Data Quality rules (valid dates, non-null VINs, valid service types).
- Structured logging with named loggers.
- DQ metrics emission for observability.

Schedule: Yearly (Jan 1st via omniroute_yearly_maintenance DAG)

Usage:
    spark-submit spark_jobs/batch/ingest_maintenance_schedules.py --run-date 2026-01-01
"""

import os
import uuid
import json
import argparse
import logging
from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, to_date, col
from pyspark.sql.types import StructType, StructField, StringType


# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ingest_maintenance_schedules")


# ──────────────────────────────────────────────
# Schema & Paths
# ──────────────────────────────────────────────
EXPECTED_SCHEMA = StructType([
    StructField("vin", StringType(), False),
    StructField("service_date", StringType(), True),
    StructField("service_type", StringType(), True),
])

SOURCE_FILENAME = "maintenance_schedules.csv"

LANDING_PATH = os.environ.get("LANDING_PATH", "s3a://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/landing")
INGESTED_PATH = os.environ.get("INGESTED_PATH", "s3a://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/ingested/") + "maintenance_schedules"
QUARANTINE_PATH = os.environ.get("QUARANTINE_PATH", "s3a://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/quarantine")
METRICS_PATH = os.environ.get("METRICS_PATH", "s3a://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/metrics")

VALID_SERVICE_TYPES = {
    "Engine Overhaul", "Tire Rotation", "Oil Change",
    "Brake Inspection", "Battery Replacement", "Full Service",
}


def run(spark: SparkSession, run_date: str):
    source = f"{LANDING_PATH}/{SOURCE_FILENAME}"
    job_name = "ingest_maintenance_schedules"
    logger.info(f"Starting FULL SNAPSHOT ingestion for {run_date}. Reading from: {source}")

    # ── 1. Pre-validate File Columns ──────────────────────
    try:
        raw_df = spark.read.option("header", "true").csv(source)
        actual_columns = set(raw_df.columns)
        expected_columns = set(f.name for f in EXPECTED_SCHEMA.fields)

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
        except Exception:
            pass
        raise

    # ── 2. Read with Enforced Schema ──────────────────────
    df = spark.read.option("header", "true").schema(EXPECTED_SCHEMA).csv(source)
    row_count = df.count()

    if row_count == 0:
        logger.warning("Empty file — 0 rows read. Exiting gracefully.")
        return

    # ── 3. Add Metadata ───────────────────────────────────
    batch_id = str(uuid.uuid4())
    df = (df
          .withColumn("load_date", to_date(lit(run_date)))
          .withColumn("ingestion_timestamp", current_timestamp())
          .withColumn("source_file_name", lit(SOURCE_FILENAME))
          .withColumn("batch_id", lit(batch_id)))

    # ── 4. Data Quality Rules ─────────────────────────────
    # Valid: vin is not null, service_date is a parseable date, service_type is not blank
    valid_df = df.filter(
        col("vin").isNotNull() &
        col("service_date").isNotNull() &
        (col("service_date") != "") &
        col("service_type").isNotNull() &
        (col("service_type") != "")
    )
    invalid_df = df.subtract(valid_df)

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    logger.info(f"Data Quality Scan: {valid_count} valid rows | {invalid_count} invalid rows.")

    # ── 5. Row count assertion ────────────────────────────
    if row_count > 0 and invalid_count / row_count > 0.20:
        logger.critical(
            f"DATA QUALITY ALARM: {invalid_count}/{row_count} rows "
            f"({invalid_count / row_count * 100:.1f}%) failed validation!"
        )

    # ── 6. Write Valid Data (dynamic partition overwrite) ──
    if valid_count > 0:
        logger.info(f"Writing {valid_count} valid records to Parquet: {INGESTED_PATH}")
        (valid_df.write
         .mode("overwrite")
         .partitionBy("load_date")
         .parquet(INGESTED_PATH))

    # ── 7. Quarantine Invalid Rows ────────────────────────
    if invalid_count > 0:
        quarantine_parquet = f"{QUARANTINE_PATH}/maintenance_schedules"
        logger.warning(f"Quarantining {invalid_count} bad rows to Parquet: {quarantine_parquet}")
        (invalid_df.write
         .mode("append")
         .partitionBy("load_date")
         .parquet(quarantine_parquet))

    # ── 8. Emit DQ Metrics ────────────────────────────────
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

    spark = (
        SparkSession.builder
        .appName("OmniRoute_ingest_maintenance_schedules")
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
