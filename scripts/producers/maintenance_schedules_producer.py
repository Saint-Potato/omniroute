import sys
import os

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
PARENT_DIR  = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

import csv
import random
import boto3
from datetime import datetime, timedelta

from config import (
    S3_LANDING_BUCKET,
    S3_LANDING_PREFIX,
    VEHICLE_REGISTRY_FILE,
    MAINTENANCE_RAW_FILE,
    MAINTENANCE_CONFIG,
    DATA_DIR
)

# ================================
# S3 CONFIG — landing zone from config.py (mirrors s3_paths.json)
# ================================
S3_BUCKET  = S3_LANDING_BUCKET
S3_KEY     = f"{S3_LANDING_PREFIX}/maintenance_schedules.csv"
S3_LANDING = f"s3://{S3_BUCKET}/{S3_KEY}"

# ================================
# CONFIG (FROM CONFIG)
# ================================

os.makedirs(DATA_DIR, exist_ok=True)

REGISTRY_FILE = VEHICLE_REGISTRY_FILE
OUTPUT_FILE = MAINTENANCE_RAW_FILE

NUM_RECORDS = MAINTENANCE_CONFIG["NUM_RECORDS"]
SERVICE_TYPES = MAINTENANCE_CONFIG["SERVICE_TYPES"]

# ================================
# LOAD VINs
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
# GENERATE DATA
# ================================

def generate_data(vins):
    data = []

    for _ in range(NUM_RECORDS):

        vin = random.choice(vins)

        start_date = datetime(2026, 1, 1)
        random_days = random.randint(0, 364)
        service_date = start_date + timedelta(days=random_days)

        record = {
            "vin": vin,
            "service_date": service_date.strftime("%Y-%m-%d"),
            "service_type": random.choice(SERVICE_TYPES)
        }

        data.append(record)

    return data


# ================================
# EDGE CASES
# ================================

def add_edge_cases(data, vins):
    total = len(data)

    # --- BRD REQUIREMENT: Maintenance Exclusion Test ---
    # We force a valid VIN (e.g. vins[0]) to have maintenance on 2026-05-10
    if len(vins) > 0:
        data.append({
            "vin": vins[0],
            "service_date": "2026-05-10",
            "service_type": "Engine Overhaul"
        })

    # 1. Duplicates (5%)
    for _ in range(int(total * 0.05)):
        data.append(random.choice(data).copy())

    # 2. Invalid VINs (5%)
    for _ in range(int(total * 0.05)):
        data.append({
            "vin": "INVALID_VIN_" + str(random.randint(1000, 9999)),
            "service_date": "2026-05-10",
            "service_type": random.choice(["Oil Change", "Brake Inspection"])
        })

    # 3. Invalid/Future Dates (5%)
    for _ in range(int(total * 0.05)):
        data.append({
            "vin": random.choice(vins) if vins else "VIN_PLACEHOLDER",
            "service_date": random.choice(["INVALID_DATE", "2099-12-31", "1800-01-01"]),
            "service_type": "Brake Inspection"
        })

    return data


# ================================
# WRITE CSV
# ================================

def write_csv(data):

    fields = ["vin", "service_date", "service_type"]

    with open(OUTPUT_FILE, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(data)

    print(f"Generated {len(data)} records → {OUTPUT_FILE}")

    # ── Upload to S3 Bronze landing/ ─────────────────────────────────
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

    print("Generating maintenance data...")
    data = generate_data(vins)

    print("Adding edge cases...")
    data = add_edge_cases(data, vins)

    random.shuffle(data)

    write_csv(data)

    print("Done!")