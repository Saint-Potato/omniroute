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
    VEHICLE_ASSIGNMENT_RAW_FILE,
    VEHICLE_ASSIGNMENT_CONFIG,
    DATA_DIR
)

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

    # Dynamic base date: starting 6 months ago from today
    now_utc = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    base_date = now_utc - timedelta(days=180)

    for vin in vins:

        num_assignments = random.randint(1, 4)

        start_date = base_date + timedelta(days=random.randint(0, 200))

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

            start_date = end_date + timedelta(days=random.randint(1, 5))

    return data


# ================================
# EDGE CASES
# ================================

def add_edge_cases(data, vins):

    now_utc = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    
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

    # Existing basic error cases
    data.append({
        "vin": "INVALID123",
        "driver_id": "DRV_BAD",
        "start_timestamp": to_unix(now_utc + timedelta(days=10)),
        "end_timestamp": "",
        "daily_rate": 500,
        "region": "South"
    })

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

    print(f"Generated {len(data)} rows  {OUTPUT_FILE}")


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