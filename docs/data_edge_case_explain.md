# OmniRoute Data Generators: Complete Team Guide

This document is the **definitive, complete guide** to all **6 data generator scripts**. It details every single piece of normal data, hidden trap, and edge case that our simulated pipelines produce. Our Data Engineering team must build Spark and Flink pipelines capable of catching and handling *all* of these scenarios perfectly.

---

## 1. Restricted Zones (`restricted_zones_producer.py`)
**Purpose:** Creates the static map of geographical "no-go" bounding boxes (e.g., Airport Zones, VIP Zones). If a truck enters these, it triggers a strike.
**Hidden Traps Planted:**
- **The Overlap Trap:** Generates two distinct zones that geographically overlap. Our spatial joins must ensure a truck isn't penalized twice for being in the overlapping area.
- **The Infinite Zone:** Generates an impossibly large bounding box to test if our spatial index crashes on extreme ranges.
- **The Ghost Zone:** Outputs a zone with coordinates way out of bounds (Latitude 200, Longitude 500).
- **The Empty Zone:** Outputs a zone with completely blank coordinates `{}` to test JSON null-handling.

---

## 2. Vehicle Registry (`vehicle_registry_producer.py`)
**Purpose:** Creates our master list of all vehicles (trucks), including their model and fuel type.
**Hidden Traps Planted:**
- **The Clones (Exact Duplicates):** Outputs perfectly duplicated rows to ensure our system uses deduplication (e.g. `.dropDuplicates()`).
- **The Missing Year:** Outputs a truck with a completely blank manufacturing year to test null-handling.
- **The Bad Fuel:** Outputs a truck that runs on `"INVALID_FUEL"` instead of Diesel or Electric to test data validation lookups.

---

## 3. Vehicle Assignments (`vehicle_assignment_producer.py`)
**Purpose:** Records which driver is driving which truck, assigning them a Start Date, End Date, and Daily Pay Rate. Updates incrementally.
**Hidden Traps Planted:**
- **The Driver Swap:** We force a scenario on April 15th where one driver ends their shift on a truck (`VIN-SWAP-TEST`) and a second driver takes it over on that *exact same day*. This tests contiguous SCD Type 2 logic.
- **The Pay Rate Conflict:** We generate two completely overlapping assignments for the exact same truck on the exact same day, but one pays $400 and the other pays $600. Our system logic must prove it honors the BRD and picks the higher rate!
- **The Ghost Truck:** Assigns a driver to a fake truck VIN (`INVALID123`) that fundamentally doesn't exist in the Registry, testing our Inner Joins.

---

## 4. Maintenance Schedules (`maintenance_schedules_producer.py`)
**Purpose:** Schedules the future days when trucks will be in the repair shop. 
**Hidden Traps Planted:**
- **The "Target Day":** We explicitly schedule a truck for an Engine Overhaul precisely on **May 10, 2026**. (We use this specific date to try and trick the Fuel Transactions test below!)
- **Random Clones & Ghost VINs:** Produces duplicate maintenance rows and maintenance tasks for `INVALID_XXXX` trucks.
- **Bad Formats:** Generates the literal text `"INVALID_DATE"` instead of a real calendar date, which tests if our PySpark date-parsers accurately catch bad casts.
- **Missing Information:** Generates a repair day but leaves the "Service Type" completely blank.

---

## 5. Fuel Transactions (`fuel_transactions_producer.py`)
**Purpose:** Tracks refueling and distance driven to audit whether drivers are burning >12% more fuel than their truck's baseline average.
**Hidden Traps Planted:**
- **The Maintenance Excuse:** Forces a terribly inefficient fuel log exactly on May 10, 2026. (Our system must realize the truck was in the repair shop that day based on the Maintenance file and *forgive* the driver!)
- **The Sunday Excuse:** Forces a terribly inefficient fuel log on May 17th (a Sunday). The system must realize it's a weekend via date functions and forgive the driver.
- **The True Flag:** Forces a highly inefficient fuel log on a random Tuesday, ensuring our reports successfully flag and catch *real* fuel abusers.
- **Random Clones (Duplicates):** Outputs exact duplicate transaction rows.
- **The Reversing Odometer:** Generates odometer readings that inexplicably jump backwards (subtracting 100 to 500 km).
- **The Bad Pump:** Generates events with `0` liters pumped, negative `-10` liters pumped, or an entirely blank text field `""` for fuel to test divide-by-zero crashes.

---

## 6. IoT Telemetry Streams (`telemetry_producer.py`)
**Purpose:** A real-time live GPS stream outputting a truck's current speed and latitude/longitude to Kafka.
**Hidden Traps Planted:**
- **The Double-Fault Deduplication Trap:** Sends an event driving at 115 km/h *while simultaneously* driving inside a Restricted Zone. Our stream system must be smart enough to apply the business logic and issue exactly 1 Safety Strike, not 2.
- **The Instant Suspension Constraint:** Rapid-fires 11 speeding events within 10 seconds for a single driver (`DRV-SUSPENSION`) to verify our system cuts them off instantly at exactly 10 strikes and emits a `SUSPENDED` flag.
- **Ghost Data (Late Arrivals):** Transmits an event stream where the timestamp claims it happened 3 entire days ago! Tests if our pipeline uses Watermarking to ignore very old, delayed data safely.
- **The Anonymous Truck (Stream-Static Join):** Outputs a Kafka event with a valid VIN but legally leaves the `driver_id` completely blank and missing. This brutally enforces the BRD requirement: "In the stream, you must join the Kafka vin with the Asset History table to find the correct driver_id currently assigned."
- **The Dead Letter Queue (DLQ) Schema Breaker:** Transmits the literal word `"WAY TOO FAST"` as string text instead of an integer number for the truck's speed, testing to ensure our JSON stream reader safely filters it out without violently crashing the master process.
