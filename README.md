## **Overview**

OmniRoute is a data engineering project that processes batch and real-time logistics data to monitor fleet operations, driver safety, and fuel efficiency.

It combines:

- Batch data from AWS S3 (vehicle registry, assignments, fuel logs)
- Real-time telemetry from Kafka (GPS, speed, sensor data)

The system generates insights such as:

- Driver safety violations
- Fuel efficiency anomalies
- Vehicle assignment history (SCD Type 2)

## **Objectives**

- Track vehicle-driver history using SCD Type 2
- Detect abnormal fuel consumption (>12% deviation)
- Monitor real-time safety violations (speeding, restricted zones)
- Apply penalty system based on safety strikes
- Generate daily and monthly reports

## **Setup Instructions**

1. Clone the repository:

```bash
git clone https://github.com/Saint-Potato/omniroute
```

2. Create virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

## **How to Run**

1. Copy Spark jobs to the expected directory:

```bash
sudo mkdir -p /opt/omniroute/spark_jobs
sudo cp -r spark_jobs/* /opt/omniroute/spark_jobs/
```

2. Copy DAGs to the Airflow DAGs folder:

```bash
cp dags/*.py ~/airflow/dags/
```

3. Load environment variables:

```bash
export $(grep -v '^#' .env | xargs)
```

4. Start Airflow:

```bash
airflow db init        # first-time only
airflow scheduler &
airflow webserver --host 0.0.0.0 --port 8080
```

### **DAG Schedules**

| DAG | Schedule | Description |
| --- | --- | --- |
| `omniroute_daily_batch` | `0 5 * * *` | Ingest vehicle registry, assignment, fuel transactions |
| `omniroute_monthly_cooldown` | `0 5 1 * *` | Reset driver strikes, generate rate deduction report |
| `omniroute_yearly_maintenance` | `0 0 1 1 *` | Ingest maintenance schedules |