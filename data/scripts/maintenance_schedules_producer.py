import sys
import os

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

import csv
import random
from datetime import datetime, timedelta

from config import (
    VEHICLE_REGISTRY_FILE,
    MAINTENANCE_RAW_FILE,
    MAINTENANCE_CONFIG,
    DATA_DIR
)

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

    for _ in range(int(total * 0.01)):
        data.append(random.choice(data).copy())

    for _ in range(int(total * 0.005)):
        data.append({
            "vin": "INVALID_" + str(random.randint(1000, 9999)),
            "service_date": "2026-05-10",
            "service_type": "Oil Change"
        })

    for _ in range(int(total * 0.005)):
        data.append({
            "vin": random.choice(vins),
            "service_date": "INVALID_DATE",
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

    print(f"Generated {len(data)} records  {OUTPUT_FILE}")


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