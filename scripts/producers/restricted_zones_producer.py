import sys
import os

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
PARENT_DIR  = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

import json
import random
import boto3

from config import (
    S3_LANDING_BUCKET,
    S3_LANDING_PREFIX,
    RESTRICTED_ZONES_RAW_FILE,
    ZONES_CONFIG,
    DATA_DIR
)

# ================================
# S3 CONFIG — landing zone from config.py (mirrors s3_paths.json)
# ================================
S3_BUCKET  = S3_LANDING_BUCKET
S3_KEY     = f"{S3_LANDING_PREFIX}/restricted_zones.json"
S3_LANDING = f"s3://{S3_BUCKET}/{S3_KEY}"

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

    # NOTE: "Too Large Zone" (1° × 2°) was removed because it covered
    # nearly the entire Delhi bounding box and caused false ZONE_INTRUSION
    # on ~100% of telemetry records, making violation detection meaningless.

    return zones


# ================================
# WRITE JSON
# ================================

def write_json(data):

    with open(OUTPUT_FILE, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Generated {len(data)} zones → {OUTPUT_FILE}")

    # ── Upload to S3 Bronze landing/ ─────────────────────────────────
    # The Silver Glue job reads restricted_zones.json from S3 landing.
    # Upload here so Glue always has the latest zone definitions.
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

    print("Generating restricted zones...")

    zones = generate_zones()
    zones = add_edge_cases(zones)

    write_json(zones)

    print("Done!")