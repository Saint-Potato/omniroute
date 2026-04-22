import os
import sys

# ================================
# PROJECT ROOT
# ================================

BASE_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))

# ================================
# DATA DIRECTORY
# ================================

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
os.makedirs(DATA_DIR, exist_ok=True)

# ================================
# RAW / PROCESSED DIRECTORIES
# ================================

RAW_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")

TELEMETRY_RAW_DIR = os.path.join(RAW_DIR, "telemetry")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(TELEMETRY_RAW_DIR, exist_ok=True)

# ================================
# FILE PATHS (CURRENT)
# ================================

VEHICLE_REGISTRY_FILE = os.path.join(DATA_DIR, "raw/vehicle_registry.csv")
VEHICLE_ASSIGNMENT_FILE = os.path.join(DATA_DIR, "raw/vehicle_assignment.csv")
MAINTENANCE_FILE = os.path.join(DATA_DIR, "raw/maintenance_schedules.csv")
FUEL_FILE = os.path.join(DATA_DIR, "raw/fuel_transactions.csv")
RESTRICTED_ZONES_FILE = os.path.join(DATA_DIR, "raw/restricted_zones.json")

# ================================
# RAW FILE PATHS (FUTURE SAFE)
# ================================

VEHICLE_REGISTRY_RAW_FILE = os.path.join(RAW_DIR, "vehicle_registry.csv")
VEHICLE_ASSIGNMENT_RAW_FILE = os.path.join(RAW_DIR, "vehicle_assignment.csv")
MAINTENANCE_RAW_FILE = os.path.join(RAW_DIR, "maintenance_schedules.csv")
FUEL_RAW_FILE = os.path.join(RAW_DIR, "fuel_transactions.csv")
RESTRICTED_ZONES_RAW_FILE = os.path.join(RAW_DIR, "restricted_zones.json")

# ================================
# SCRIPT PATHS
# ================================

DATA_SCRIPTS_DIR = BASE_DIR
STREAMING_SCRIPTS_DIR = BASE_DIR

# ================================
# EXECUTION CONFIG
# ================================

PYTHON_EXEC = sys.executable

# ================================
# VEHICLE REGISTRY CONFIG
# ================================

VEHICLE_REGISTRY_CONFIG = {
    "NUM_RECORDS": 500,
    "MODELS": [
        "Freightliner M2",
        "Volvo VNL",
        "Isuzu N-Series",
        "Tata Ultra",
        "Ashok Leyland Dost"
    ],
    "FUEL_TYPES": ["Diesel", "LNG", "CNG", "Electric"]
}

# ================================
# VEHICLE ASSIGNMENT CONFIG
# ================================

VEHICLE_ASSIGNMENT_CONFIG = {
    "NUM_ROWS": 1800,
    "REGIONS": ["North", "South", "East", "West", "Central", "NCR", "Mumbai", "Jaipur"],
    "MIN_RATE": 300,
    "MAX_RATE": 1200
}

# ================================
# MAINTENANCE CONFIG
# ================================

MAINTENANCE_CONFIG = {
    "NUM_RECORDS": 1000,
    "SERVICE_TYPES": [
        "Engine Overhaul",
        "Tire Rotation",
        "Oil Change",
        "Brake Inspection",
        "Battery Replacement",
        "Full Service"
    ]
}

# ================================
# FUEL TRANSACTION CONFIG
# ================================

FUEL_CONFIG = {
    "NUM_RECORDS": 25000,
    "MIN_FUEL": 20,
    "MAX_FUEL": 150,
    "MIN_DISTANCE": 100,
    "MAX_DISTANCE": 500
}

# ================================
# RESTRICTED ZONES CONFIG
# ================================

ZONES_CONFIG = {
    "NUM_ZONES": 8,
    "LAT_RANGE": (28.5, 28.8),
    "LONG_RANGE": (77.0, 77.4),
    "OFFSET_RANGE": (0.01, 0.03)
}

# ================================
# TELEMETRY CONFIG (DYNAMIC KAFKA)
# ================================

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")

TELEMETRY_CONFIG = {
    "KAFKA_TOPIC": "vehicle_telemetry_topic",
    "KAFKA_SERVER": KAFKA_SERVER,
    "EVENT_DELAY": 1,
    "LAT_RANGE": (28.4, 28.9),
    "LONG_RANGE": (76.8, 77.5),
    "NORMAL_SPEED": (40, 100),
    "HIGH_SPEED": (110, 130),
    "EXTREME_SPEED": (130, 160)
}

# ================================
# OPTIONAL: SCHEMA CONTROL
# ================================

TELEMETRY_SCHEMA = [
    "vin",
    "driver_id",
    "speed",
    "lat",
    "long",
    "event_timestamp"
]

# ================================
# PIPELINE CONTROL
# ================================

PIPELINE_CONFIG = {
    "RUN_TELEMETRY": True,
    "ENABLE_EDGE_CASES": True
}

# ================================
# POSTGRES REPORTING EXPORT
# ================================

POSTGRES_CONFIG = {
    "HOST": os.getenv("PG_HOST", "localhost"),
    "PORT": os.getenv("PG_PORT", "5432"),
    "DATABASE": os.getenv("PG_DB", "omniroute_dwh"),
    "USER": os.getenv("PG_USER", "postgres"),
    "PASSWORD": os.getenv("PG_PASSWORD", "postgres"),
    "DRIVER_STATUS_TABLE": "gold.driver_safety_status"
}
