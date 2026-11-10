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
    VEHICLE_REGISTRY_CONFIG,
    FUEL_RAW_FILE,
    FUEL_CONFIG,
    DATA_DIR
)

# ================================
# S3 CONFIG — landing zone from config.py (mirrors s3_paths.json)
# ================================
S3_BUCKET  = S3_LANDING_BUCKET
S3_KEY     = f"{S3_LANDING_PREFIX}/fuel_transactions.csv"
S3_LANDING = f"s3://{S3_BUCKET}/{S3_KEY}"

# ================================
# CONFIG (FROM CONFIG)
# ================================

os.makedirs(DATA_DIR, exist_ok=True)

REGISTRY_FILE = VEHICLE_REGISTRY_FILE
OUTPUT_FILE = FUEL_RAW_FILE

NUM_RECORDS = FUEL_CONFIG["NUM_RECORDS"]
MIN_FUEL = FUEL_CONFIG["MIN_FUEL"]
MAX_FUEL = FUEL_CONFIG["MAX_FUEL"]
MIN_DISTANCE = FUEL_CONFIG["MIN_DISTANCE"]
MAX_DISTANCE = FUEL_CONFIG["MAX_DISTANCE"]

# ================================
# LOAD VINs
# ================================

def load_vins():
    vin_model_map = {}

    if not os.path.exists(REGISTRY_FILE):
        raise FileNotFoundError(f"Registry file not found at: {REGISTRY_FILE}")

    with open(REGISTRY_FILE, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            vin_model_map[row["vin"]] = row["model"]

    print(f"Loaded {len(vin_model_map)} VINs")
    return vin_model_map


# ================================
# GENERATE DATA
# ================================

def generate_data(vin_model_map):
    data = []
    txn_counter = 1

    # Dynamic date anchoring for realistic pipeline processing
    now = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = now - timedelta(days=1)
    base_date = datetime(2026, 1, 1)  # fallback range start for the 10%

    vins = list(vin_model_map.keys())
    odometer_map = {vin: random.randint(10000, 50000) for vin in vins}

    # Use centralized baseline from config (single source of truth)
    BASELINE_KM_PER_LITER = VEHICLE_REGISTRY_CONFIG["BASELINE_KM_PER_LITER"]

    for _ in range(NUM_RECORDS):

        vin = random.choice(vins)
        model = vin_model_map[vin]
        baseline = BASELINE_KM_PER_LITER.get(model, 5.0)

        distance = random.randint(MIN_DISTANCE, MAX_DISTANCE)
        odometer_map[vin] += distance

        if random.random() < 0.10:
            efficiency = baseline * 0.85
        else:
            efficiency = baseline * random.uniform(0.95, 1.05)

        fuel_liters = round(distance / efficiency, 2)

        # ── Date Distribution ────────────────────────────────────────
        # 90% yesterday (current_date - 1) — realistic batch ingestion
        # 10% random historical range — backfill / stress-test
        if random.random() < 0.90:
            timestamp = yesterday + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
        else:
            timestamp = base_date + timedelta(
                days=random.randint(0, 120),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )

        record = {
            "transaction_id": f"TXN_{txn_counter}",
            "vin": vin,
            "fuel_liters": fuel_liters,
            "odometer_reading": odometer_map[vin],
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S")
        }

        txn_counter += 1
        data.append(record)

    return data


# ================================
# EDGE CASES
# ================================

def add_edge_cases(data, vins):
    total = len(data)

    if len(vins) > 1:
        # --- BRD REQUIREMENT: Maintenance Exclusion Test ---
        data.append({
            "transaction_id": "TXN_MAINT_TEST",
            "vin": vins[0],  # Matches the maintenance generated VIN from other script
            "fuel_liters": 200, 
            "odometer_reading": 30500, # Ensures terrible efficiency
            "timestamp": "2026-05-10 14:00:00"
        })

        # --- BRD REQUIREMENT: The Weekend Exclusion Test (2026-05-17 is a Sunday) ---
        data.append({
            "transaction_id": "TXN_WEEKEND_TEST",
            "vin": vins[1],
            "fuel_liters": 200, 
            "odometer_reading": 40500, 
            "timestamp": "2026-05-17 14:00:00"
        })
        
        # --- BRD REQUIREMENT: The Baseline Deviation Anomaly (Weekday) ---
        # This is a guaranteed strike for the Fuel Audit
        data.append({
            "transaction_id": "TXN_BAD_EFF_TEST",
            "vin": vins[0],         
            "fuel_liters": 200, 
            "odometer_reading": 50500, 
            "timestamp": "2026-05-12 14:00:00" # Tuesday
        })

    # 1. Duplicates (2%)
    for _ in range(int(total * 0.02)):
        data.append(random.choice(data).copy())

    # 2. Data Quality Issues: 0 or Negative Fuel (5%)
    for _ in range(int(total * 0.05)):
        row = random.choice(data).copy()
        row["transaction_id"] = f"TXN_DQ_{random.randint(1000, 9999)}"
        row["fuel_liters"] = random.choice([0, -10, -50.5])
        data.append(row)

    # 3. Odometer Fraud / Rollback (5%)
    for _ in range(int(total * 0.05)):
        row = random.choice(data).copy()
        row["transaction_id"] = f"TXN_FRAUD_{random.randint(1000, 9999)}"
        # Subtracting a large amount to ensure it is lower than the previous reading
        row["odometer_reading"] -= random.randint(500, 5000)
        data.append(row)

    # 4. Severe Fuel Efficiency Anomalies for Fuel Audit (3%)
    # Very high fuel consumption for the distance covered
    for _ in range(int(total * 0.03)):
        row = random.choice(data).copy()
        row["transaction_id"] = f"TXN_ANOMALY_{random.randint(1000, 9999)}"
        row["fuel_liters"] *= random.uniform(2.5, 4.0) # 250% to 400% worse efficiency
        data.append(row)

    return data


# ================================
# WRITE CSV
# ================================

def write_csv(data):

    fields = ["transaction_id", "vin", "fuel_liters", "odometer_reading", "timestamp"]

    with open(OUTPUT_FILE, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(data)

    print(f"Generated {len(data)} rows → {OUTPUT_FILE}")

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
    vin_model_map = load_vins()
    vins_list = list(vin_model_map.keys())

    print("Generating fuel transactions...")
    data = generate_data(vin_model_map)

    print("Adding edge cases...")
    data = add_edge_cases(data, vins_list)

    random.shuffle(data)

    write_csv(data)

    print("Done!")