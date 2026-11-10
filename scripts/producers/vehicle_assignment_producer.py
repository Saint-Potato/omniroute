import sys
import os

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
PARENT_DIR  = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

import csv
import json
import random
from datetime import datetime, timedelta
import boto3

from config import (
    S3_LANDING_BUCKET,
    S3_LANDING_PREFIX,
    VEHICLE_REGISTRY_FILE,
    VEHICLE_ASSIGNMENT_RAW_FILE,
    VEHICLE_ASSIGNMENT_CONFIG,
    DATA_DIR
)

# ================================
# S3 CONFIG — landing zone from config.py (mirrors s3_paths.json)
# ================================
S3_BUCKET  = S3_LANDING_BUCKET
S3_KEY     = f"{S3_LANDING_PREFIX}/vehicle_assignment.csv"
S3_LANDING = f"s3://{S3_BUCKET}/{S3_KEY}"

# ================================
# CONFIG (FROM CONFIG)
# ================================

os.makedirs(DATA_DIR, exist_ok=True)

REGISTRY_FILE = VEHICLE_REGISTRY_FILE
OUTPUT_FILE = VEHICLE_ASSIGNMENT_RAW_FILE

NUM_ROWS = VEHICLE_ASSIGNMENT_CONFIG["NUM_ROWS"]
REGIONS = VEHICLE_ASSIGNMENT_CONFIG["REGIONS"]

# ================================
# LOAD VINs FROM REGISTRY
# ================================

def load_vins():
    vins = []

    if not os.path.exists(REGISTRY_FILE):
        raise FileNotFoundError(f"Registry file not found at: {REGISTRY_FILE}")

    with open(REGISTRY_FILE, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            vins.append(row["vin"])

    print(f"Loaded {len(vins)} VINs")
    return vins


# ================================
# HELPER FUNCTIONS
# ================================

def generate_driver_id(num):
    return f"DRV_{str(num).zfill(5)}"


def to_unix(dt):
    return int(dt.timestamp())


# ================================
# MAIN GENERATION LOGIC
# ================================

def generate_assignments(vins):
    data = []
    driver_counter = 1

    # Dynamic base date anchoring for realistic pipeline processing
    now_utc = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    for vin in vins:

        num_assignments = random.randint(1, 4)

        for i in range(num_assignments):

            driver_id = generate_driver_id(driver_counter)
            driver_counter += 1

            daily_rate = round(
                random.uniform(
                    VEHICLE_ASSIGNMENT_CONFIG["MIN_RATE"],
                    VEHICLE_ASSIGNMENT_CONFIG["MAX_RATE"]
                ), 2
            )

            region = random.choice(REGIONS)

            # ── Date Distribution ────────────────────────────────────
            # 85% current date   — main workload for today's pipeline
            #  5% future          — upcoming assignments (today+1..+30)
            # 10% past            — historical backfill   (today-180..-1)
            roll = random.random()
            if roll < 0.85:
                start_date = now_utc + timedelta(
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59)
                )
            elif roll < 0.90:
                start_date = now_utc + timedelta(
                    days=random.randint(1, 30),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59)
                )
            else:
                start_date = now_utc - timedelta(
                    days=random.randint(1, 180),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59)
                )

            duration = random.randint(10, 60)
            end_date = start_date + timedelta(days=duration)

            if i == num_assignments - 1:
                end_timestamp = ""
            else:
                end_timestamp = to_unix(end_date)

            record = {
                "vin": vin,
                "driver_id": driver_id,
                "start_timestamp": to_unix(start_date),
                "end_timestamp": end_timestamp,
                "daily_rate": daily_rate,
                "region": region
            }

            data.append(record)

    return data


# ================================
# EDGE CASES
# ================================

def add_edge_cases(data, vins):

    now_utc = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    total = len(data)
    
    # --- BRD REQUIREMENT: The Driver Swap ---
    swap_date = to_unix(datetime(2026, 4, 15))
    data.append({
        "vin": "VIN-SWAP-TEST",
        "driver_id": "DRV_SWAP_1",
        "start_timestamp": to_unix(datetime(2026, 4, 1)),
        "end_timestamp": swap_date,
        "daily_rate": 500.0,
        "region": "North"
    })
    data.append({
        "vin": "VIN-SWAP-TEST",
        "driver_id": "DRV_SWAP_2",
        "start_timestamp": swap_date,
        "end_timestamp": "",
        "daily_rate": 550.0,
        "region": "North"
    })

    # --- BRD REQUIREMENT: Conflict Resolution ---
    conflict_date = to_unix(datetime(2026, 5, 20))
    data.append({
        "vin": "VIN-CONFLICT-TEST",
        "driver_id": "DRV_CONF_A",
        "start_timestamp": conflict_date,
        "end_timestamp": "",
        "daily_rate": 400.0,
        "region": "South"
    })
    data.append({
        "vin": "VIN-CONFLICT-TEST",
        "driver_id": "DRV_CONF_B",
        "start_timestamp": conflict_date,
        "end_timestamp": "",
        "daily_rate": 600.0,
        "region": "South"
    })

    # 1. SCD2 Overlaps / Conflicts (10%)
    # Same VIN, different drivers, completely overlapping timeframes
    for _ in range(int(total * 0.10)):
        row = random.choice(data).copy()
        row["driver_id"] = f"DRV_OVERLAP_{random.randint(1000, 9999)}"
        # Keep same vin, same start_timestamp to force a conflict in the downstream SCD2 logic
        data.append(row)

    # 2. Orphaned Assignments (5%)
    # Assignment for a VIN that doesn't exist in the Vehicle Registry
    for _ in range(int(total * 0.05)):
        row = random.choice(data).copy()
        row["vin"] = f"VIN-ORPHAN-{random.randint(100, 999)}"
        row["driver_id"] = f"DRV_ORPHAN_{random.randint(1000, 9999)}"
        data.append(row)

    # 3. Invalid Dates (5%)
    # Start date is AFTER the end date
    for _ in range(int(total * 0.05)):
        row = random.choice(data).copy()
        if row["end_timestamp"] != "":
            # Swap start and end
            temp = row["start_timestamp"]
            row["start_timestamp"] = row["end_timestamp"]
            row["end_timestamp"] = temp
            data.append(row)

    return data


# ================================
# WRITE CSV
# ================================

def write_csv(data):

    fields = ["vin", "driver_id", "start_timestamp", "end_timestamp", "daily_rate", "region"]

    with open(OUTPUT_FILE, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(data)

    print(f"Generated {len(data)} rows → {OUTPUT_FILE}")

    # ── Upload to S3 Bronze landing/ ─────────────────────────────────
    # The Silver Glue job reads vehicle_assignment.csv directly from S3.
    # We must upload here so Glue always has the latest reference data.
    try:
        s3 = boto3.client("s3")
        s3.upload_file(OUTPUT_FILE, S3_BUCKET, S3_KEY)
        print(f"Uploaded to S3 → {S3_LANDING}")
    except Exception as e:
        print(f"WARNING: S3 upload failed: {e}")
        print(f"Manual upload required: aws s3 cp {OUTPUT_FILE} {S3_LANDING}")


# ================================
# MAIN
# ================================

if __name__ == "__main__":

    print("Loading VINs...")
    vins = load_vins()

    print("Generating assignments...")
    data = generate_assignments(vins)

    print("Adding edge cases...")
    data = add_edge_cases(data, vins)

    random.shuffle(data)

    write_csv(data)

    print("Done! Ready for pipeline.")