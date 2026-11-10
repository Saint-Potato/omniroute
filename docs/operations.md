# OmniRoute — Operations Guide

> Airflow configuration, daily runbook, and troubleshooting playbook for the OmniRoute data platform.

---

## Table of Contents

- [Airflow Variables](#airflow-variables)
- [Airflow Connections](#airflow-connections)
- [Runbook](#runbook)
  - [DAG 1 — Midnight Pipeline](#dag-1--midnight-pipeline)
  - [DAG 2 — Morning Fuel Pipeline](#dag-2--morning-fuel-pipeline)
  - [DAG 3 — EMR Streaming Pipeline](#dag-3--emr-streaming-pipeline)
- [Troubleshooting](#troubleshooting)
  - [Midnight Pipeline Issues](#midnight-pipeline-issues)
  - [Morning Fuel Pipeline Issues](#morning-fuel-pipeline-issues)
  - [EMR Streaming Pipeline Issues](#emr-streaming-pipeline-issues)
  - [Glue Job Failures](#glue-job-failures)
  - [PostgreSQL Connectivity](#postgresql-connectivity)
  - [S3 / Data Issues](#s3--data-issues)

---

## Airflow Variables

Set these via the Airflow UI (**Admin → Variables**) or import them in bulk:

```bash
airflow variables import config/airflow_variables.json
```

### Infrastructure Variables

| Variable | Example | Description |
|---|---|---|
| `emr_subnet_id` | `subnet-0abc123def456` | VPC subnet used when provisioning the EMR cluster. Must have outbound internet access for package downloads and S3 connectivity. |

### Pipeline Config Variable

| Variable | Example | Description |
|---|---|---|
| `omniroute_config` | contents of `config/s3_paths.json` | JSON with all S3 paths and Glue job settings. Read when DAG files are parsed; if missing or invalid, all three DAGs show an import error. Update it with `airflow variables set omniroute_config "$(cat config/s3_paths.json)"` after editing the file. |

### PostgreSQL Variables

These are injected into Glue jobs at runtime via Jinja templating (`{{ var.value.pg_host }}`).

| Variable | Example | Description |
|---|---|---|
| `pg_host` | `omniroute-pg.abc123.us-east-1.rds.amazonaws.com` | PostgreSQL host address |
| `pg_port` | `5432` | PostgreSQL port (defaults to `5432` if unset) |
| `pg_database` | `omniroute_reporting` | Target reporting database |
| `pg_user` | `omniroute_user` | Database username |
| `pg_password` | *(secret)* | Database password — consider using Airflow's **Secrets Backend** in production |

### Auto-Managed Gate Variables

These variables are **set and read automatically** by the DAGs to implement idempotent retry logic. You should only touch them manually during incident recovery.

| Variable | Format | Managed By | Description |
|---|---|---|---|
| `omniroute_yearly_done_YYYY` | `omniroute_yearly_done_2026` | Midnight Pipeline | Set to `"true"` after yearly tasks (maintenance schedules + dim_date) complete successfully. Prevents re-execution for the rest of the year. |
| `omniroute_monthly_done_YYYY_MM` | `omniroute_monthly_done_2026_05` | Midnight Pipeline | Set to `"true"` after monthly tasks (rate deduction + strikes reset) complete successfully. Prevents re-execution for the rest of that month. |

**How the gates work:**

1. On each DAG run, a `BranchPythonOperator` checks whether the Variable exists and equals `"true"`.
2. If `"true"` → the branch is skipped entirely.
3. If absent or `"false"` → the branch runs, and on success a `PythonOperator` sets it to `"true"`.
4. If the branch **fails**, the Variable stays unset → the tasks will retry on the next DAG run automatically.

---

## Airflow Connections

Set these via the Airflow UI (**Admin → Connections**) or import:

```bash
airflow connections import config/airflow_connections.json
```

| Connection ID | Type | Region | Auth Method | Used By |
|---|---|---|---|---|
| `aws_default` | Amazon Web Services | `us-east-1` | Instance Profile / IAM Role (no explicit keys) | All 3 DAGs — Glue operators, EMR operators, S3 access |

> **Note:** The `aws_default` connection relies on the IAM role attached to the Airflow instance (EC2 instance profile or ECS task role). No `aws_access_key_id` / `aws_secret_access_key` is stored in Airflow.

### Required IAM Permissions

The IAM role used by `aws_default` needs at minimum:

| Service | Permissions |
|---|---|
| **S3** | `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` on all Bronze/Silver/Gold buckets |
| **Glue** | `glue:CreateJob`, `glue:StartJobRun`, `glue:GetJobRun`, `glue:GetJob`, `glue:UpdateJob` |
| **EMR** | `elasticmapreduce:RunJobFlow`, `elasticmapreduce:AddJobFlowSteps`, `elasticmapreduce:DescribeStep`, `elasticmapreduce:TerminateJobFlows` |
| **IAM** | `iam:PassRole` for the Glue and EMR service roles |

---

## Runbook

### DAG 1 — Midnight Pipeline

| Property | Value |
|---|---|
| **DAG ID** | `omniroute_midnight_pipeline` |
| **Schedule** | `0 0 * * *` (daily at 00:00 UTC) |
| **Max Active Runs** | 1 |
| **Default Retries** | 2 (5 min delay) |
| **Catchup** | Disabled |

#### What It Does

1. **Yearly branch** — Runs once per year (Jan 1st), retries daily until success:
   - `yearly_bronze_ingest` → ingests `maintenance_schedules.csv`
   - `silver_maintenance_schedules` → normalizes maintenance data
   - `silver_dim_date` → generates the calendar dimension for the year
   - `mark_yearly_done` → sets `omniroute_yearly_done_YYYY = "true"`

2. **Daily tasks** — Run every day:
   - `daily_bronze_midnight` → ingests `vehicle_registry.csv` + `vehicle_assignment.csv`
   - `daily_safety_snapshot` → archives current driver safety status from PostgreSQL to S3

3. **Silver/Gold branch** — Sequential after safety snapshot:
   - `silver_vehicle_registry` → dedup, type-casting, enrichment
   - `silver_vehicle_assignment` → SCD Type 2 merge
   - `gold_active_fleet_snapshot` → daily IN-TRANSIT vehicle count by model

4. **Monthly branch** — Runs on the 1st of each month, retries daily until success:
   - `monthly_rate_deduction_report` → payroll deduction report from accumulated strikes
   - `safety_strikes_reset` → resets strikes & restores rates (excludes SUSPENDED drivers with ≥10 strikes)
   - `mark_monthly_done` → sets `omniroute_monthly_done_YYYY_MM = "true"`

#### Manual Intervention Scenarios

| Scenario | Action |
|---|---|
| Yearly tasks failed and you fixed the root cause mid-year | Ensure `omniroute_yearly_done_YYYY` is **deleted** or set to `"false"`, then let the next midnight run pick it up. |
| Monthly tasks need re-running | Delete `omniroute_monthly_done_YYYY_MM` from Airflow Variables, then trigger the DAG or wait for the next scheduled run. |
| Need to skip yearly tasks for the year | Set `omniroute_yearly_done_YYYY = "true"` manually. |

---

### DAG 2 — Morning Fuel Pipeline

| Property | Value |
|---|---|
| **DAG ID** | `omniroute_morning_fuel_pipeline` |
| **Schedule** | `0 5 * * *` (daily at 05:00 UTC) |
| **Max Active Runs** | 1 |
| **Default Retries** | 2 (5 min delay) |
| **Catchup** | Disabled |
| **Upstream Dependency** | Midnight Pipeline (conceptual — `ExternalTaskSensor` is present but currently commented out) |

#### What It Does

1. `daily_bronze_fuel` → ingests only `fuel_transactions.csv` from the landing zone
2. `silver_fuel_transactions` → cleanses fuel data, joins with maintenance/assignment/vehicle tables
3. `gold_fuel_efficiency_audit` → flags vehicles consuming >12% above fleet baseline (excludes weekends & maintenance days)
4. `gold_to_postgres` → syncs all Gold + Silver dimension tables to the PostgreSQL reporting schema

#### Important Notes

- The `ExternalTaskSensor` (waits for midnight pipeline's `end` task) is **currently commented out**. This means the morning pipeline runs independently. If re-enabled, it uses a 5-hour `execution_delta` to align logical dates.
- The `gold_to_postgres` task needs all five PostgreSQL variables (`pg_host`, `pg_port`, `pg_database`, `pg_user`, `pg_password`) to be set.

---

### DAG 3 — EMR Streaming Pipeline

| Property | Value |
|---|---|
| **DAG ID** | `omniroute_emr_streaming_pipeline` |
| **Schedule** | `None` (manual trigger only) |
| **Max Active Runs** | 1 |
| **Default Retries** | 1 (5 min delay) |
| **Catchup** | Disabled |

#### What It Does

1. `create_emr_cluster` → provisions a transient EMR 7.12.0 cluster (1 Primary + 3 Core + 1 Task, all `m5a.xlarge`)
2. `add_streaming_steps` → submits 3 Spark Structured Streaming steps concurrently (Bronze, Silver, Gold)
3. `watch_*_step` → 3 parallel `EmrStepSensor` tasks monitoring each step (polls every 60s, timeout 1h)
4. `terminate_emr_cluster` → terminates the cluster (runs even if steps fail, via `trigger_rule="all_done"`)

#### Cluster Details

| Property | Value |
|---|---|
| **Release** | `emr-7.12.0` |
| **Primary** | `m5a.xlarge × 1` (64 GB EBS) |
| **Core** | `m5a.xlarge × 3` (64 GB EBS each) |
| **Task** | `m5a.xlarge × 1` (64 GB EBS) |
| **Step Concurrency** | 3 |
| **Applications** | Hadoop, Hive, Spark, Livy, JupyterEnterpriseGateway |
| **Bootstrap** | `bootstrap_omniroute.sh` — installs Python deps |
| **Kafka Broker** | `172.31.65.131:9092` (hardcoded) |
| **Termination Protection** | Disabled |
| **Log URI** | `s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/EMR-logs/` |

#### Pre-Flight Checklist

Before triggering the EMR DAG:

- [ ] Verify `emr_subnet_id` Variable is set to the correct subnet
- [ ] Verify Kafka broker at `172.31.65.131:9092` is reachable from the subnet
- [ ] Verify streaming scripts are uploaded to `s3://.../emr/`
- [ ] Verify `omniroute_libs.zip` has been built and uploaded (run `emr_package.sh`)
- [ ] Verify `bootstrap_omniroute.sh` is uploaded to the EMR S3 path

---

## Troubleshooting

### Midnight Pipeline Issues

#### Yearly tasks keep running every day

**Symptom:** `yearly_bronze_ingest` runs daily instead of only once.

**Cause:** The `mark_yearly_done` task is failing silently, so `omniroute_yearly_done_YYYY` never gets set.

**Fix:**
1. Check logs for `mark_yearly_done` in the Airflow UI.
2. If the upstream tasks succeeded but the marker failed, manually set the Variable:
   ```
   Airflow UI → Admin → Variables → Add
   Key:   omniroute_yearly_done_2026
   Value: true
   ```

#### Monthly tasks didn't run on the 1st

**Symptom:** `monthly_rate_deduction_report` was skipped on the 1st of the month.

**Cause:** A stale `omniroute_monthly_done_YYYY_MM` Variable from a previous partial run.

**Fix:**
1. Delete the Variable for the current month:
   ```
   Airflow UI → Admin → Variables → Delete omniroute_monthly_done_2026_05
   ```
2. Trigger the midnight pipeline manually or wait for the next 00:00 UTC run.

#### `daily_safety_snapshot` fails with PostgreSQL error

**Symptom:** Task fails with `Connection refused` or `password authentication failed`.

**Cause:** PostgreSQL Variables are misconfigured.

**Fix:**
1. Verify all 5 PG Variables are set correctly in the Airflow UI.
2. Test connectivity from the Glue VPC to the PostgreSQL host:
   - Ensure security groups allow inbound on `pg_port` from the Glue ENI subnet.
3. Re-run the failed task from the Airflow UI (click task → **Clear**).

---

### Morning Fuel Pipeline Issues

#### `gold_to_postgres` fails with "relation does not exist"

**Symptom:** `UndefinedTable: relation "report.fuel_efficiency_audit" does not exist`.

**Cause:** The PostgreSQL DDL hasn't been applied.

**Fix:**
```bash
psql -h <host> -U omniroute_user -d omniroute_reporting -f sql/reporting_ddl.sql
```

#### `silver_fuel_transactions` produces 0 records

**Symptom:** Gold audit table is empty despite fuel data existing in Bronze.

**Cause:** The Silver transform joins with `silver.vehicle_assignment` and `silver.vehicle_registry` — if the midnight pipeline hasn't run yet (or failed), those tables may be empty or stale.

**Fix:**
1. Check that the midnight pipeline succeeded for the same `run_date`.
2. Verify Bronze `fuel_transactions` data exists at the expected partition path.
3. If the `ExternalTaskSensor` is commented out, the fuel pipeline may have run before midnight completed — consider re-enabling it.

---

### EMR Streaming Pipeline Issues

#### Cluster creation fails

**Symptom:** `create_emr_cluster` fails with `ValidationException` or subnet errors.

**Causes & Fixes:**

| Cause | Fix |
|---|---|
| `emr_subnet_id` Variable not set or invalid | Set a valid subnet ID that belongs to the correct VPC. |
| IAM role `AmazonEMRServiceRole` doesn't exist | Create it via the AWS Console: EMR → Set up EMR service role. |
| EC2 instance limit reached | Request a limit increase for `m5a.xlarge` in `us-east-1`. |
| Security group IDs don't exist | Update `EmrManagedMasterSecurityGroup` and `EmrManagedSlaveSecurityGroup` in `EMR_DAG.py`. |

#### Streaming steps fail immediately

**Symptom:** `watch_*_step` shows `FAILED` status within seconds.

**Causes & Fixes:**

| Cause | Fix |
|---|---|
| Scripts not found on S3 | Re-upload streaming scripts to `s3://.../emr/` (see [Getting Started](../README.md#6-deploy-streaming-scripts-to-emr-s3)). |
| `omniroute_libs.zip` missing | Run `bash emr_package.sh` and upload the zip. |
| Bootstrap script failed | Check EMR logs at the `LogUri` path. Common issue: pip install failure due to no internet access from the subnet. |
| Kafka broker unreachable | Verify `172.31.65.131:9092` is accessible from the EMR subnet. Check security groups and NACLs. |

#### Cluster not terminated after failure

**Symptom:** EMR cluster stays running after streaming steps fail.

**Expected behavior:** The `terminate_emr_cluster` task uses `trigger_rule="all_done"`, so it should run regardless of step outcomes.

**Fix:** If it somehow didn't terminate:
1. Terminate manually via AWS Console: EMR → Clusters → Select cluster → Terminate.
2. Check Airflow logs for the `terminate_emr_cluster` task to see why it didn't execute.

---

### Glue Job Failures

#### General Glue debugging steps

1. **Check Airflow task logs** — the `GlueJobOperator` prints the Glue job run ID.
2. **Check Glue console** — AWS Console → Glue → Jobs → select the job → Run details → Logs.
3. **Check CloudWatch** — Glue logs are written to `/aws-glue/jobs/output` and `/aws-glue/jobs/error` log groups.

#### Common Glue errors

| Error | Cause | Fix |
|---|---|---|
| `Script not found at location` | Script not uploaded to the job's derived script location | Upload the script: `aws s3 cp spark_jobs/batch/<script>.py <artifacts.glue_scripts.<layer>><script>` (layer + script from `glue.jobs.<key>` in `s3_paths.json`) |
| `AnalysisException: Path does not exist` | Input data missing at the expected S3 path | Verify upstream jobs ran and the data exists at the Bronze/Silver path. |
| `MERGE INTO ... target table not found` | First run — Delta table hasn't been created yet | The Silver transforms auto-create the table on first write. Check for other errors in the logs. |
| `GlueVersion not supported` | Wrong Glue version in `s3_paths.json` | Ensure `glue.defaults.glue_version` is `"4.0"` and no job overrides it. |
| `Max concurrent runs exceeded` | Previous job run is still in progress | Wait for the existing run to finish or cancel it in the Glue console. |

#### Retry behavior

- All Glue tasks are configured with `MaxRetries: 1` at the Glue level.
- Airflow retries each task **2 times** with a **5-minute delay** (`default_args`).
- The `daily_bronze_midnight` and `daily_bronze_fuel` tasks have **task-level overrides**: 2 retries with a 3-minute delay.

---

### PostgreSQL Connectivity

#### Connection test

From any machine with network access to the PostgreSQL host:

```bash
psql -h <pg_host> -p <pg_port> -U <pg_user> -d <pg_database> -c "SELECT 1;"
```

#### Common issues

| Symptom | Cause | Fix |
|---|---|---|
| `Connection timed out` | Security group doesn't allow inbound from Glue ENIs | Add an inbound rule for TCP port `5432` from the Glue subnet CIDR. |
| `FATAL: password authentication failed` | Wrong `pg_user` or `pg_password` Variable | Update the Airflow Variables with the correct credentials. |
| `FATAL: database "omniroute_reporting" does not exist` | Database hasn't been created | `CREATE DATABASE omniroute_reporting;` then apply DDL. |
| `psycopg2.OperationalError: could not translate host name` | `pg_host` Variable has a typo or DNS isn't resolving | Verify the hostname and ensure DNS resolution works from the Glue VPC. |

---

### S3 / Data Issues

#### Quarantined records

Records that fail schema validation during Bronze ingestion are written to:

```
s3://snowflake-logs-bucket-jai/test-data/quarantine/
```

Check quarantined data to understand what was rejected:

```bash
aws s3 ls s3://snowflake-logs-bucket-jai/test-data/quarantine/ --recursive
```

#### Missing landing files

If Bronze ingestion finds no files, the task will succeed with 0 records processed (no error).

Check that source files are being deposited to the landing zone:

```bash
aws s3 ls s3://snowflake-logs-bucket-jai/test-data/landing/
```

#### Delta Lake table corruption

If a Delta table becomes corrupted (e.g., partial write from a killed Glue job):

1. Check the Delta transaction log:
   ```bash
   aws s3 ls s3://<table_path>/_delta_log/ --recursive
   ```
2. If needed, delete the incomplete checkpoint and re-run the job.
3. As a last resort, delete the entire table directory and re-run the full pipeline from Bronze.

---

## Quick Reference — DAG Schedule

| DAG | Schedule | Trigger |
|---|---|---|
| `omniroute_midnight_pipeline` | `0 0 * * *` | Automatic (daily 00:00 UTC) |
| `omniroute_morning_fuel_pipeline` | `0 5 * * *` | Automatic (daily 05:00 UTC) |
| `omniroute_emr_streaming_pipeline` | `None` | Manual only |
