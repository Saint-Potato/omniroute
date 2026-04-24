import sys
import os

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

import csv
import random
import string
from datetime import datetime

from config import (
    VEHICLE_REGISTRY_RAW_FILE,
    VEHICLE_REGISTRY_CONFIG,
    DATA_DIR
)

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

    if len(data) > 2:

        duplicate = data[0].copy()
        data.append(duplicate)

        bad_record_1 = data[1].copy()
        bad_record_1["mfg_year"] = ""
        data.append(bad_record_1)

        bad_record_2 = data[2].copy()
        bad_record_2["fuel_type"] = "INVALID_FUEL"
        data.append(bad_record_2)

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


# ================================
# MAIN EXECUTION
# ================================

if __name__ == "__main__":

    print("Generating Vehicle Registry Data...")

    data = generate_vehicle_registry()

    data = add_edge_cases(data)

    write_to_csv(data)

    print("Done!")