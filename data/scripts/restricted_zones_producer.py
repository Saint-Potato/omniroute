import sys
import os

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

import json
import random

from config import (
    RESTRICTED_ZONES_RAW_FILE,
    ZONES_CONFIG,
    DATA_DIR
)

# ================================
# CONFIG (FROM CONFIG)
# ================================

os.makedirs(DATA_DIR, exist_ok=True)

OUTPUT_FILE = RESTRICTED_ZONES_RAW_FILE

NUM_ZONES = ZONES_CONFIG["NUM_ZONES"]
LAT_RANGE = ZONES_CONFIG["LAT_RANGE"]
LONG_RANGE = ZONES_CONFIG["LONG_RANGE"]
OFFSET_RANGE = ZONES_CONFIG["OFFSET_RANGE"]

# ================================
# REALISTIC ZONE NAMES
# ================================

ZONE_NAMES = [
    "IGI Airport Zone",
    "Delhi Cantonment",
    "No Entry CP",
    "Industrial Restricted Zone",
    "Military Base Area",
    "VIP Movement Zone",
    "Warehouse Restricted Area",
    "High Security Zone"
]

# ================================
# GENERATE VALID ZONES
# ================================

def generate_zones():

    zones = []

    for _ in range(NUM_ZONES):

        center_lat = random.uniform(LAT_RANGE[0], LAT_RANGE[1])
        center_long = random.uniform(LONG_RANGE[0], LONG_RANGE[1])

        lat_offset = random.uniform(OFFSET_RANGE[0], OFFSET_RANGE[1])
        long_offset = random.uniform(OFFSET_RANGE[0], OFFSET_RANGE[1])

        zone = {
            "zone_name": random.choice(ZONE_NAMES),
            "min_lat": round(center_lat - lat_offset, 6),
            "max_lat": round(center_lat + lat_offset, 6),
            "min_long": round(center_long - long_offset, 6),
            "max_long": round(center_long + long_offset, 6)
        }

        zones.append(zone)

    return zones


# ================================
# EDGE CASES
# ================================

def add_edge_cases(zones):

    zones.append({
        "zone_name": "Overlap Zone",
        "min_lat": 28.55,
        "max_lat": 28.60,
        "min_long": 77.08,
        "max_long": 77.12
    })

    zones.append({
        "zone_name": "Overlap Zone 2",
        "min_lat": 28.57,
        "max_lat": 28.62,
        "min_long": 77.10,
        "max_long": 77.15
    })

    zones.append({
        "zone_name": "Invalid Zone",
        "min_lat": 200,
        "max_lat": 300,
        "min_long": 500,
        "max_long": 600
    })

    zones.append({})

    zones.append({
        "zone_name": "Too Large Zone",
        "min_lat": 28.0,
        "max_lat": 29.0,
        "min_long": 76.0,
        "max_long": 78.0
    })

    return zones


# ================================
# WRITE JSON
# ================================

def write_json(data):

    with open(OUTPUT_FILE, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Generated {len(data)} zones  {OUTPUT_FILE}")


# ================================
# MAIN
# ================================

if __name__ == "__main__":

    print("Generating restricted zones...")

    zones = generate_zones()
    zones = add_edge_cases(zones)

    write_json(zones)

    print("Done!")