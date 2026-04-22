import sys
import os

CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))

if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

import subprocess

# ================================
# BASE PATH (PRODUCTION READY)
# ================================

BASE_DIR = ROOT_DIR

DATA_SCRIPTS = CURRENT_DIR
STREAMING_SCRIPTS = CURRENT_DIR

PYTHON_EXEC = sys.executable


# ================================
# HELPER FUNCTION
# ================================

def run_script(script_path, name):

    print(f"\nRunning: {name}")

    if not os.path.exists(script_path):
        print(f"ERROR: Script not found → {script_path}")
        sys.exit(1)

    try:
        result = subprocess.run(
            [PYTHON_EXEC, script_path],
            check=True,
            capture_output=True,
            text=True
        )

        print(f"{name} completed successfully")
        print(result.stdout)

    except subprocess.CalledProcessError as e:
        print(f"ERROR in {name}")
        print(e.stderr)
        sys.exit(1)


# ================================
# MAIN PIPELINE
# ================================

def main():

    print("\nSTARTING FULL DATA PIPELINE\n")

    scripts = [
        ("vehicle_registry_producer.py", "Vehicle Registry"),
        ("vehicle_assignment_producer.py", "Vehicle Assignment"),
        ("maintenance_schedules_producer.py", "Maintenance Schedules"),
        ("fuel_transactions_producer.py", "Fuel Transactions"),
        ("restricted_zones_producer.py", "Restricted Zones"),
    ]

    for script, name in scripts:
        run_script(os.path.join(DATA_SCRIPTS, script), name)

    print("\nALL BATCH DATA GENERATED SUCCESSFULLY")

    start_stream = input("\nDo you want to start telemetry streaming? (y/n): ").strip().lower()

    if start_stream == "y":
        print("\nStarting Real-Time Telemetry (Press CTRL+C to stop)\n")

        telemetry_script = os.path.join(STREAMING_SCRIPTS, "telemetry_producer.py")

        if not os.path.exists(telemetry_script):
            print(f"ERROR: Telemetry script not found → {telemetry_script}")
            sys.exit(1)

        subprocess.run([PYTHON_EXEC, telemetry_script])

    else:
        print("\nSkipping telemetry streaming")


# ================================
# ENTRY POINT
# ================================

if __name__ == "__main__":
    main()