"""
===========================================================
OmniRoute Smart Logistics Engine — Telemetry Consumer V2
===========================================================

Production-grade Kafka consumer that implements the FULL
streaming pipeline required by the BRD:

  1. Raw Ingestion        → Parquet (date-partitioned)
  2. Violation Detection  → Speed > 110 km/h + Geofence
  3. Safety Strike Count  → Stateful per-driver tracking
  4. Penalty Calculation  → 5% deduction per strike
  5. Suspension Toggling  → 10 strikes → SUSPENDED
  6. Gold Layer Output    → driver_safety_status (Parquet)
  7. Postgres Export      → Reporting Layer (gold.driver_safety_status)

State is persisted to disk (JSON) for crash recovery.
Offsets are committed ONLY after successful writes to ensure Idempotency.
"""

import json
import time
import csv
import logging
import os
from datetime import datetime, timedelta
import sys
import pandas as pd

# PostgreSQL support
try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    psycopg2 = None

# Kafka is only required when actually running the consumer
try:
    from kafka import KafkaConsumer
except ImportError:
    KafkaConsumer = None

# Add parent dir to sys.path directly to ensure imports work correctly
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

from config import (
    TELEMETRY_CONFIG,
    TELEMETRY_RAW_DIR,
    VEHICLE_ASSIGNMENT_FILE,
    RESTRICTED_ZONES_FILE,
    PROCESSED_DIR,
    POSTGRES_CONFIG,
)

# ================================
# LOGGING SETUP
# ================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("TelemetryConsumerV2")

AUDIT_LOG_FILE = os.path.join(CURRENT_DIR, "omniroute_audit.txt")

# ================================
# CONSTANTS (FROM BRD)
# ================================

SPEED_THRESHOLD = 110            # km/h — BRD Section 3.3
PENALTY_RATE = 0.05              # 5% deduction per strike — BRD Section 3.3
SUSPENSION_STRIKE_LIMIT = 10     # 10 strikes → SUSPENDED — BRD Section 3.3
BATCH_SIZE = 500                 # Events per batch write
COOLDOWN_HOUR = 5                # 05:00 UTC — BRD Section 3.3

# ================================
# KAFKA CONFIG
# ================================

KAFKA_TOPIC = TELEMETRY_CONFIG["KAFKA_TOPIC"]
KAFKA_SERVER = TELEMETRY_CONFIG["KAFKA_SERVER"]

# ================================
# OUTPUT DIRECTORIES
# ================================

VIOLATIONS_DIR = os.path.join(PROCESSED_DIR, "violations")
SAFETY_STATUS_DIR = os.path.join(PROCESSED_DIR, "driver_safety_status")
DLQ_DIR = os.path.join(TELEMETRY_RAW_DIR, "dlq")
STATE_FILE = os.path.join(PROCESSED_DIR, "strike_state.json")

os.makedirs(VIOLATIONS_DIR, exist_ok=True)
os.makedirs(SAFETY_STATUS_DIR, exist_ok=True)
os.makedirs(DLQ_DIR, exist_ok=True)

# ================================================================
# 1. REFERENCE DATA LOADERS (WITH CACHING)
# ================================================================

_assignment_cache = {
    "data": {},
    "last_mtime": 0
}

def load_restricted_zones():
    """Load valid restricted zones from JSON."""
    zones = []
    if not os.path.exists(RESTRICTED_ZONES_FILE):
        logger.warning(f"Restricted zones file not found: {RESTRICTED_ZONES_FILE}")
        return zones

    try:
        with open(RESTRICTED_ZONES_FILE, "r") as f:
            raw_zones = json.load(f)
    except Exception as e:
        logger.error(f"Failed to load restricted zones: {e}")
        return zones

    for z in raw_zones:
        if not z or "min_lat" not in z or "max_lat" not in z:
            continue
        if abs(z["min_lat"]) > 90 or abs(z["max_lat"]) > 90:
            continue
        if abs(z["min_long"]) > 180 or abs(z["max_long"]) > 180:
            continue
        if (z["max_lat"] - z["min_lat"]) > 0.5 or (z["max_long"] - z["min_long"]) > 0.5:
            continue

        zones.append(z)

    logger.info(f"Loaded {len(zones)} valid restricted zones")
    return zones


def load_active_assignments(force_refresh=False):
    """Load the VIN → (driver_id, daily_rate) mapping with caching."""
    global _assignment_cache

    if not os.path.exists(VEHICLE_ASSIGNMENT_FILE):
        return {}

    mtime = os.path.getmtime(VEHICLE_ASSIGNMENT_FILE)
    if not force_refresh and mtime <= _assignment_cache["last_mtime"]:
        return _assignment_cache["data"]
 
    mapping = {}
    current_time = int(time.time())
    
    with open(VEHICLE_ASSIGNMENT_FILE, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            end_ts = row["end_timestamp"].strip()
            
            # Record is active if no end_timestamp or it's in the future
            if end_ts == "" or (end_ts.isdigit() and int(end_ts) > current_time):
                vin = row["vin"]
                try:
                    daily_rate = float(row["daily_rate"])
                except (ValueError, TypeError):
                    daily_rate = 0.0

                if vin not in mapping or daily_rate > mapping[vin]["daily_rate"]:
                    mapping[vin] = {
                        "driver_id": row["driver_id"],
                        "daily_rate": daily_rate
                    }

    _assignment_cache["data"] = mapping
    _assignment_cache["last_mtime"] = mtime
    logger.info(f"Loaded {len(mapping)} active assignments into cache")
    return mapping


# ================================================================
# 2. STATE MANAGEMENT & COOLDOWN (BRD LOGIC)
# ================================================================

def get_effective_month(event_ts=None):
    """
    BRD Section 3.3: Cool down happens on the 1st of the month at 05:00 UTC.
    Uses event_timestamp if provided, otherwise fallback to current time.
    """
    if event_ts:
        now = datetime.utcfromtimestamp(event_ts)
    else:
        now = datetime.utcnow()
        
    if now.day == 1 and now.hour < COOLDOWN_HOUR:
        return (now - timedelta(days=1)).strftime("%Y-%m")
    return now.strftime("%Y-%m")


def load_strike_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
            logger.info(f"Loaded strike state for {len(state)} drivers")
            return state
        except Exception as e:
            logger.error(f"Failed to load state file: {e}. Starting fresh.")
    return {}


def save_strike_state(state):
    tmp_file = STATE_FILE + ".tmp"
    with open(tmp_file, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp_file, STATE_FILE)


def log_audit(message):
    """Append a message to the omniroute_audit.txt file."""
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    try:
        with open(AUDIT_LOG_FILE, "a") as f:
            f.write(f"\n[{timestamp}] STREAMING: {message}")
    except Exception as e:
        logger.error(f"Audit log failed: {e}")


def check_monthly_cooldown(state, max_event_ts=None):
    """
    Apply cooldown reset if we cross into a new effective month.
    """
    effective_month = get_effective_month(max_event_ts)
    reset_count = 0

    for driver_id, info in state.items():
        # Only reset if advancing into a new month properly and month is set
        if info.get("month", "") != effective_month and info.get("month", "") != "":
            # BRD: Drivers who accumulate 10 strikes (SUSPENDED) are excluded from the cooldown
            if info.get("status") == "SUSPENDED":
                info["month"] = effective_month  # Just update the timestamp marker
                continue

            # Eligible drivers reset 
            info["strike_count"] = 0
            info["status"] = "ACTIVE"
            info["month"] = effective_month
            reset_count += 1

    if reset_count > 0:
        msg = f"Monthly cooldown: Reset strikes for {reset_count} eligible drivers for {effective_month}"
        logger.info(msg)
        log_audit(msg)
        save_strike_state(state)

    return state


# ================================================================
# 3. VIOLATION DETECTION
# ================================================================

def is_in_restricted_zone(lat, lon, zones):
    for zone in zones:
        if (zone["min_lat"] <= lat <= zone["max_lat"] and
                zone["min_long"] <= lon <= zone["max_long"]):
            return True, zone.get("zone_name", "UNKNOWN")
    return False, None


def detect_violations(event, zones):
    """
    BRD Section 3.3:
    Flag events speed > 110 km/h or coordinates intersect restricted zones.
    A single event window = ONE strike max, even if both happen.
    """
    violation_types = []
    zone_name = None

    try:
        speed = float(event.get("speed", 0))
        lat = float(event.get("lat", 0.0))
        lon = float(event.get("long", 0.0))
    except (ValueError, TypeError):
        return False, [], None

    if speed > SPEED_THRESHOLD:
        violation_types.append("SPEED_VIOLATION")

    in_zone, matched_zone = is_in_restricted_zone(lat, lon, zones)
    if in_zone:
        violation_types.append("ZONE_INTRUSION")
        zone_name = matched_zone

    return len(violation_types) > 0, violation_types, zone_name


def apply_strike(driver_id, state, event_ts=None):
    effective_month = get_effective_month(event_ts)
    if driver_id not in state:
        state[driver_id] = {
            "strike_count": 0, "status": "ACTIVE", "month": effective_month
        }

    driver_state = state[driver_id]
    if driver_state["status"] == "SUSPENDED":
        return driver_state

    # Cap strikes exactly at SUSPENSION_STRIKE_LIMIT
    driver_state["strike_count"] = min(driver_state["strike_count"] + 1, SUSPENSION_STRIKE_LIMIT)
    
    if driver_state["strike_count"] >= SUSPENSION_STRIKE_LIMIT:
        driver_state["status"] = "SUSPENDED"
        msg = f"SUSPENSION: Driver {driver_id} hit {SUSPENSION_STRIKE_LIMIT} strikes and is now suspended."
        logger.warning(msg)
        log_audit(msg)

    # Re-stamp state with effective month
    driver_state["month"] = effective_month
    return driver_state


def calculate_adjusted_rate(base_rate, strike_count):
    deduction = base_rate * PENALTY_RATE * strike_count
    return round(max(base_rate - deduction, 0.0), 2)


# ================================================================
# 4. POSTGRES EXPORT (GOLD REPORTING)
# ================================================================

def sync_to_postgres(records):
    """BRD Section 6.2 - Reporting Database."""
    if not psycopg2 or not records:
        return

    try:
        conn = psycopg2.connect(
            host=POSTGRES_CONFIG["HOST"],
            port=POSTGRES_CONFIG["PORT"],
            database=POSTGRES_CONFIG["DATABASE"],
            user=POSTGRES_CONFIG["USER"],
            password=POSTGRES_CONFIG["PASSWORD"],
            connect_timeout=3
        )
        cur = conn.cursor()
        
        query = f"""
            INSERT INTO {POSTGRES_CONFIG["DRIVER_STATUS_TABLE"]} 
            (driver_id, base_rate, strike_count, current_adjusted_rate, status, month)
            VALUES %s
            ON CONFLICT (driver_id) DO UPDATE SET
            strike_count = EXCLUDED.strike_count,
            current_adjusted_rate = EXCLUDED.current_adjusted_rate,
            status = EXCLUDED.status,
            month = EXCLUDED.month;
        """
        
        data = [
            (r['driver_id'], r['base_rate'], r['strike_count'], 
             r['current_adjusted_rate'], r['status'], r['month']) 
            for r in records
        ]
        
        execute_values(cur, query, data)
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Synced {len(records)} active drivers to PostgreSQL")
    except Exception as e:
        # Don't crash pipeline if reporting DB is down
        logger.debug(f"PostgreSQL sync skipped/failed: {e}")


# ================================================================
# 5. PARQUET WRITERS (BRONZE/SILVER/GOLD)
# ================================================================

def get_partition_path(base_dir, event_timestamp):
    dt = pd.to_datetime(event_timestamp)
    path = os.path.join(base_dir, f"year={dt.year}", f"month={str(dt.month).zfill(2)}", f"day={str(dt.day).zfill(2)}")
    os.makedirs(path, exist_ok=True)
    return path


def atomic_parquet_write(df, file_path):
    """Write to .tmp first, then rename safely."""
    tmp_path = file_path + ".tmp"
    df.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, file_path)


def write_dlq_events(dlq_batch):
    """Write malformed events to JSON Dead Letter Queue."""
    if not dlq_batch: return
    file_path = os.path.join(DLQ_DIR, f"dlq_{int(time.time() * 1000)}.json")
    with open(file_path, "w") as f:
        json.dump(dlq_batch, f)


def write_raw_telemetry(batch, min_offset, max_offset):
    if not batch: return
    df = pd.DataFrame(batch)
    df.drop_duplicates(subset=["vin", "event_timestamp"], inplace=True)
    df["_date"] = pd.to_datetime(df["event_timestamp"]).dt.date
    for date_val, group_df in df.groupby("_date"):
        partition_dir = get_partition_path(TELEMETRY_RAW_DIR, str(date_val))
        file_name = f"telemetry_offset_{min_offset}_to_{max_offset}.parquet"
        atomic_parquet_write(group_df.drop(columns=["_date"]), os.path.join(partition_dir, file_name))


def write_violation_events(violations, min_offset, max_offset):
    if not violations: return
    df = pd.DataFrame(violations)
    df["_date"] = pd.to_datetime(df["event_timestamp"]).dt.date
    for date_val, group_df in df.groupby("_date"):
        partition_dir = get_partition_path(VIOLATIONS_DIR, str(date_val))
        file_name = f"violations_offset_{min_offset}_to_{max_offset}.parquet"
        atomic_parquet_write(group_df.drop(columns=["_date"]), os.path.join(partition_dir, file_name))


def write_driver_safety_status(state, assignments, event_ts=None):
    records = []
    effective_month = get_effective_month(event_ts)
    driver_rates = {v["driver_id"]: v["daily_rate"] for v in assignments.values()}

    for driver_id, info in state.items():
        base_rate = driver_rates.get(driver_id, 0.0)
        strike_count = info.get("strike_count", 0)
        records.append({
            "driver_id": driver_id,
            "base_rate": base_rate,
            "strike_count": strike_count,
            "current_adjusted_rate": calculate_adjusted_rate(base_rate, strike_count),
            "status": info.get("status", "ACTIVE"),
            "month": info.get("month", effective_month)
        })

    if not records: return
    df = pd.DataFrame(records)
    path = os.path.join(SAFETY_STATUS_DIR, f"driver_safety_status_{effective_month}.parquet")
    atomic_parquet_write(df, path)
    
    tmp_csv = path.replace(".parquet", ".csv") + ".tmp"
    df.to_csv(tmp_csv, index=False)
    os.replace(tmp_csv, path.replace(".parquet", ".csv"))
    
    sync_to_postgres(records)


# ================================================================
# 6. MAIN KAFKA PIPELINE LOOP
# ================================================================

def run_consumer():
    logger.info("OmniRoute Telemetry Consumer V2 Starting...")
    zones = load_restricted_zones()
    assignments = load_active_assignments(force_refresh=True)
    
    strike_state = load_strike_state()
    strike_state = check_monthly_cooldown(strike_state, None)

    if KafkaConsumer is None:
        logger.error("kafka-python not installed. Cannot start consumer.")
        return

    consumer = KafkaConsumer(
        KAFKA_TOPIC, bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        enable_auto_commit=False, auto_offset_reset="earliest",
        group_id="omniroute_consumer_group_v2"
    )

    batch, violations_batch, dlq_batch = [], [], []
    min_offset, max_offset = None, None
    max_event_ts = None
    last_flush_time = time.time()

    try:
        while True:
            messages = consumer.poll(timeout_ms=1000)
            if not messages:
                # Still check for cooldown transition using system time if no traffic
                strike_state = check_monthly_cooldown(strike_state, None)
                continue

            for tp, msgs in messages.items():
                for message in msgs:
                    # Update offset range
                    if min_offset is None or message.offset < min_offset:
                        min_offset = message.offset
                    if max_offset is None or message.offset > max_offset:
                        max_offset = message.offset

                    try:
                        event = message.value
                    except Exception:
                        continue

                    # DLQ Logic: Validate critical fields
                    if "vin" not in event or "event_timestamp" not in event: 
                        dlq_batch.append(event)
                        continue

                    # Event Time tracking
                    try:
                        ts = pd.to_datetime(event["event_timestamp"]).timestamp()
                        if max_event_ts is None or ts > max_event_ts:
                            max_event_ts = ts
                    except Exception:
                        pass

                    vin = event["vin"]
                    assign = assignments.get(vin, {"driver_id": "DRV_UNKNOWN", "daily_rate": 0.0})
                    driver_id = assign["driver_id"]
                    event["driver_id"] = driver_id

                    is_v, v_types, z_name = detect_violations(event, zones)
                    if is_v:
                        violations_batch.append({
                            **event, "violation_type": "|".join(v_types), "zone_name": z_name or ""
                        })
                        if driver_id != "DRV_UNKNOWN":
                            apply_strike(driver_id, strike_state, max_event_ts)
                    
                    batch.append(event)

            # Flush criteria (BRD strictly enforces Idempotency, commit only after load)
            if len(batch) >= BATCH_SIZE or (time.time() - last_flush_time > 60 and (batch or dlq_batch)):
                strike_state = check_monthly_cooldown(strike_state, max_event_ts)
                assignments = load_active_assignments()

                write_dlq_events(dlq_batch)
                write_raw_telemetry(batch, min_offset, max_offset)
                write_violation_events(violations_batch, min_offset, max_offset)
                write_driver_safety_status(strike_state, assignments, max_event_ts)
                save_strike_state(strike_state)
                
                consumer.commit()
                logger.info(f"Batch flushed: {len(batch)} events, {len(violations_batch)} violations, {len(dlq_batch)} DLQ. Offsets: {min_offset}-{max_offset}")
                
                batch.clear()
                violations_batch.clear()
                dlq_batch.clear()
                min_offset, max_offset, max_event_ts = None, None, None
                last_flush_time = time.time()

    except KeyboardInterrupt:
        logger.info("Stopping pipeline gracefully...")
        if batch or dlq_batch:
            write_dlq_events(dlq_batch)
            write_raw_telemetry(batch, min_offset, max_offset)
            write_violation_events(violations_batch, min_offset, max_offset)
            write_driver_safety_status(strike_state, assignments, max_event_ts)
            save_strike_state(strike_state)
            consumer.commit()
    finally:
        consumer.close()


# ================================================================
# ENTRY POINT
# ================================================================

if __name__ == "__main__":
    run_consumer()
