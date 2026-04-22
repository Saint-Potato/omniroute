import sys
import os

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

import json
import time
import random
import csv
from datetime import datetime
from kafka import KafkaProducer

from config import (
    VEHICLE_REGISTRY_FILE,
    VEHICLE_ASSIGNMENT_FILE,
    TELEMETRY_CONFIG,
    RESTRICTED_ZONES_FILE
)

# ================================
# CONFIG (FROM CONFIG)
# ================================

REGISTRY_FILE = VEHICLE_REGISTRY_FILE
ASSIGNMENT_FILE = VEHICLE_ASSIGNMENT_FILE

KAFKA_TOPIC = TELEMETRY_CONFIG["KAFKA_TOPIC"]
KAFKA_SERVER = TELEMETRY_CONFIG["KAFKA_SERVER"]
EVENT_DELAY = TELEMETRY_CONFIG["EVENT_DELAY"]

LAT_RANGE = TELEMETRY_CONFIG["LAT_RANGE"]
LONG_RANGE = TELEMETRY_CONFIG["LONG_RANGE"]

NORMAL_SPEED = TELEMETRY_CONFIG["NORMAL_SPEED"]
HIGH_SPEED = TELEMETRY_CONFIG["HIGH_SPEED"]
EXTREME_SPEED = TELEMETRY_CONFIG["EXTREME_SPEED"]

# ================================
# LOAD DATA
# ================================

def load_vins():
    vins = []
    with open(REGISTRY_FILE, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            vins.append(row["vin"])
    return vins


_assignment_cache = {
    "data": {},
    "last_mtime": 0
}

def load_active_assignments(force_refresh=False):
    global _assignment_cache

    if not os.path.exists(ASSIGNMENT_FILE):
        return {}

    mtime = os.path.getmtime(ASSIGNMENT_FILE)
    if not force_refresh and mtime <= _assignment_cache["last_mtime"]:
        return _assignment_cache["data"]

    mapping = {}
    current_time = int(time.time())

    with open(ASSIGNMENT_FILE, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            end_ts = row["end_timestamp"]
            if end_ts == "" or (end_ts.isdigit() and int(end_ts) > current_time):
                mapping[row["vin"]] = row["driver_id"]

    _assignment_cache["data"] = mapping
    _assignment_cache["last_mtime"] = mtime
    return mapping


def load_restricted_zones():
    zones = []
    if os.path.exists(RESTRICTED_ZONES_FILE):
        with open(RESTRICTED_ZONES_FILE, "r") as f:
            data = json.load(f)
            for z in data:
                if "min_lat" in z:
                    zones.append(z)
    return zones


# ================================
# GENERATE TELEMETRY EVENT
# ================================

def generate_event(vin, driver_id, zones):

    r = random.random()

    if r < 0.80:
        speed = random.randint(NORMAL_SPEED[0], NORMAL_SPEED[1])
        force_intrusion = False
    elif r < 0.95:
        speed = random.randint(HIGH_SPEED[0], HIGH_SPEED[1])
        force_intrusion = (random.random() < 0.5) and len(zones) > 0
    else:
        speed = random.randint(EXTREME_SPEED[0], EXTREME_SPEED[1])
        force_intrusion = len(zones) > 0

    if force_intrusion and zones:
        zone = random.choice(zones)
        lat = round(random.uniform(zone["min_lat"], zone["max_lat"]), 6)
        long = round(random.uniform(zone["min_long"], zone["max_long"]), 6)
    else:
        lat = round(random.uniform(LAT_RANGE[0], LAT_RANGE[1]), 6)
        long = round(random.uniform(LONG_RANGE[0], LONG_RANGE[1]), 6)

    event = {
        "vin": vin,
        "driver_id": driver_id,
        "speed": speed,
        "lat": lat,
        "long": long,
        "event_timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }

    return event


def inject_edge_cases(producer, topic_name, zones):
    # 1. Double Violation Cap Test
    # speed > 110 AND inside restricted zone
    zone = zones[0] if zones else {"min_lat": 10, "max_lat": 11, "min_long": 10, "max_long": 11}
    event_double = {
        "vin": "VIN-DOUBLE-VIOLATION",
        "driver_id": "DRV_DOUBLE",
        "speed": 115,
        "lat": zone["min_lat"] + 0.0001,
        "long": zone["min_long"] + 0.0001,
        "event_timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }
    producer.send(topic_name, key=b"VIN-DOUBLE-VIOLATION", value=event_double)
    
    # 2. Suspension Test (11 strikes in a row)
    for i in range(11):
        event_suspension = {
            "vin": "VIN-SUSPENSION",
            "driver_id": "DRV-SUSPENSION",
            "speed": 125,
            "lat": 0,
            "long": 0,
            "event_timestamp": (datetime.utcnow() + timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S")
        }
        producer.send(topic_name, key=b"VIN-SUSPENSION", value=event_suspension)
    
    # 3. Late arriving data
    event_late = {
        "vin": "VIN-LATE",
        "driver_id": "DRV_LATE",
        "speed": 60,
        "lat": 0, "long": 0,
        "event_timestamp": (datetime.utcnow() - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
    }
    producer.send(topic_name, key=b"VIN-LATE", value=event_late)
    
    # 3.5 The Anonymous Truck (Missing Driver-ID to force Stream-Static Join)
    event_anonymous = {
        "vin": "1HGBH225", # A real VIN from the registry sample
        "driver_id": "",   # Missing! The stream MUST join with Asset History to find out who this is.
        "speed": 115,
        "lat": 0, "long": 0,
        "event_timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }
    producer.send(topic_name, key=b"1HGBH225", value=event_anonymous)

    # 4. Bad JSON / Schema Breaker for DLQ
    event_dlq = {
        "driver_id": "DRV_BAD",
        "speed": "WAY TOO FAST", 
        "lat": 0, "long": 0,
        "event_timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }
    producer.send(topic_name, key=b"DLQ_TEST", value=event_dlq)
    
    producer.flush()
    print("Injected advanced BRD edge cases into Kafka.")

# ================================
# MAIN PRODUCER
# ================================

def run_producer():

    print("Connecting to Kafka...")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print("Loading data...")

    vins = load_vins()
    assignments = load_active_assignments(force_refresh=True)
    zones = load_restricted_zones()

    print(f"Loaded {len(vins)} vehicles")
    print(f"Loaded {len(assignments)} active assignments")
    print(f"Loaded {len(zones)} restricted zones")

    # Inject static BRD edge cases before the infinite loop
    inject_edge_cases(producer, KAFKA_TOPIC, zones)

    print("Starting telemetry stream in batches... Press Ctrl+C to stop.\n")
    BATCH_SIZE = 50
    last_cache_check = time.time()

    try:
        while True:
            # Refresh assignments every 60 seconds
            if time.time() - last_cache_check > 60:
                assignments = load_active_assignments()
                last_cache_check = time.time()

            batch_events = []
            for _ in range(BATCH_SIZE):
                vin = random.choice(vins)
                driver_id = assignments.get(vin, "DRV_UNKNOWN")

                event = generate_event(vin, driver_id, zones)
                # Assign partition key to guarantee ordered processing downstream
                producer.send(KAFKA_TOPIC, key=vin.encode("utf-8"), value=event)
                batch_events.append(event)

            print(f"Sent batch of {BATCH_SIZE} events. Example: {batch_events[0]}")
            time.sleep(EVENT_DELAY)

    except KeyboardInterrupt:
        print("\nPipeline stopping gracefully. Flushing events to Kafka...")
    finally:
        producer.flush()
        producer.close()
        print("Kafka Producer closed safely.")


# ================================
# ENTRY POINT
# ================================

if __name__ == "__main__":
    run_producer()