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
    FUEL_RAW_FILE,
    FUEL_CONFIG,
    DATA_DIR
)

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

    base_date = datetime(2026, 1, 1)

    vins = list(vin_model_map.keys())
    odometer_map = {vin: random.randint(10000, 50000) for vin in vins}

    BASELINE_KM_PER_LITER = {
        "Freightliner M2": 5.0,
        "Volvo VNL": 4.5,
        "Isuzu N-Series": 6.0,
        "Tata Ultra": 5.5,
        "Ashok Leyland Dost": 8.0
    }

    for _ in range(NUM_RECORDS):

        vin = random.choice(vins)
        model = vin_model_map[vin]
        baseline = BASELINE_KM_PER_LITER.get(model, 5.0)

        distance = random.randint(MIN_DISTANCE, MAX_DISTANCE)
        odometer_map[vin] += distance

        if random.random() < 0.05:
            efficiency = baseline * 0.85
        else:
            efficiency = baseline * random.uniform(0.95, 1.05)

        fuel_liters = round(distance / efficiency, 2)

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
        data.append({
            "transaction_id": "TXN_BAD_EFF_TEST",
            "vin": vins[0],         
            "fuel_liters": 200, 
            "odometer_reading": 50500, 
            "timestamp": "2026-05-12 14:00:00" # Tuesday
        })

    for _ in range(int(total * 0.01)):
        data.append(random.choice(data).copy())

    for _ in range(int(total * 0.005)):
        row = random.choice(data).copy()
        row["fuel_liters"] = random.choice([0, -10])
        data.append(row)

    for _ in range(int(total * 0.005)):
        row = random.choice(data).copy()
        row["odometer_reading"] -= random.randint(100, 500)
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

    print(f"Generated {len(data)} rows  {OUTPUT_FILE}")


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