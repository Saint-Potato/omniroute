import sys
import os

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
PARENT_DIR  = os.path.abspath(os.path.join(CURRENT_DIR, ".."))  # scripts/ folder where config.py lives
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

import csv
import random
import string
import boto3
from datetime import datetime

from config import (
    S3_LANDING_BUCKET,
    S3_LANDING_PREFIX,
    VEHICLE_REGISTRY_RAW_FILE,
    VEHICLE_REGISTRY_CONFIG,
    DATA_DIR
)

# ================================
# S3 CONFIG — landing zone from config.py (mirrors s3_paths.json)
# ================================
S3_BUCKET  = S3_LANDING_BUCKET
S3_KEY     = f"{S3_LANDING_PREFIX}/vehicle_registry.csv"
S3_LANDING = f"s3://{S3_BUCKET}/{S3_KEY}"

# ================================
# BASELINE LOOKUP (FROM CONFIG)
# ================================

BASELINE_KM_PER_LITER = VEHICLE_REGISTRY_CONFIG["BASELINE_KM_PER_LITER"]

# ================================
# CONFIGURATION (FROM CONFIG)
# ================================

os.makedirs(DATA_DIR, exist_ok=True)

OUTPUT_FILE = VEHICLE_REGISTRY_RAW_FILE

NUM_RECORDS = VEHICLE_REGISTRY_CONFIG["NUM_RECORDS"]
MODELS = VEHICLE_REGISTRY_CONFIG["MODELS"]
FUEL_TYPES = VEHICLE_REGISTRY_CONFIG["FUEL_TYPES"]

# ================================
# HELPER FUNCTIONS
# ================================

def generate_vin(existing_vins):
    while True:
        vin = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        if vin not in existing_vins:
            return vin


def generate_mfg_year():
    return random.randint(2005, 2025)


def generate_model():
    return random.choice(MODELS)


def generate_fuel_type():
    return random.choice(FUEL_TYPES)


def generate_baseline_kmpl(model):
    """Return the fleet baseline km/liter for the given model."""
    return BASELINE_KM_PER_LITER.get(model, 5.0)


# ================================
# MAIN DATA GENERATION
# ================================

def generate_vehicle_registry():
    existing_vins = set()
    data = []

    for _ in range(NUM_RECORDS):

        vin = generate_vin(existing_vins)
        existing_vins.add(vin)

        model = generate_model()

        record = {
            "vin": vin,
            "model": model,
            "mfg_year": generate_mfg_year(),
            "fuel_type": generate_fuel_type(),
            "baseline_kmpl": generate_baseline_kmpl(model)
        }

        data.append(record)

    return data


# ================================
# EDGE CASES
# ================================

def add_edge_cases(data):
    total = len(data)

    # 1. Duplicates with conflicting baselines (5%)
    for _ in range(int(total * 0.05)):
        row = random.choice(data).copy()
        row["baseline_kmpl"] = row["baseline_kmpl"] * 1.5 # Conflicting baseline
        data.append(row)

    # 2. Missing required fields (5%)
    for _ in range(int(total * 0.05)):
        row = random.choice(data).copy()
        field_to_remove = random.choice(["mfg_year", "baseline_kmpl"])
        row[field_to_remove] = ""
        data.append(row)

    # 3. Invalid Enums (5%)
    for _ in range(int(total * 0.05)):
        row = random.choice(data).copy()
        row["fuel_type"] = random.choice(["Uranium", "Water", "Plutonium", "Solar", "UNKNOWN_TYPE"])
        data.append(row)

    return data


# ================================
# WRITE TO CSV
# ================================

def write_to_csv(data):

    fieldnames = ["vin", "model", "mfg_year", "fuel_type", "baseline_kmpl"]

    with open(OUTPUT_FILE, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(data)

    print(f"CSV file generated: {OUTPUT_FILE}")
    print(f"Total records: {len(data)}")

    # ── Upload to S3 Bronze landing/ ─────────────────────────────────
    try:
        s3 = boto3.client("s3")
        s3.upload_file(OUTPUT_FILE, S3_BUCKET, S3_KEY)
        print(f"Uploaded to S3 → {S3_LANDING}")
    except Exception as e:
        print(f"WARNING: S3 upload failed: {e}")
        print(f"Manual upload required: aws s3 cp {OUTPUT_FILE} {S3_LANDING}")


# ================================
# MAIN EXECUTION
# ================================

if __name__ == "__main__":

    print("Generating Vehicle Registry Data...")

    data = generate_vehicle_registry()

    data = add_edge_cases(data)

    write_to_csv(data)

    print("Done!")