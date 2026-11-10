"""
OmniRoute — EMR Streaming Pipeline DAG
========================================
Schedule: Triggered manually (or set a cron as needed)

Creates an EMR cluster and submits three Spark Streaming steps
concurrently:
  1. Bronze Streaming  — Kafka → S3 Bronze (Delta)
  2. Silver Streaming  — Bronze → S3 Silver (Delta)
  3. Gold Streaming    — Silver → S3 Gold  (Delta)

DAG Dependency Graph:
═══════════════════════
  start
    │
    ▼
  create_emr_cluster
    │
    ▼
  add_streaming_steps  (submits all 3 steps at once)
    │
    ├──► watch_bronze_step ──┐
    ├──► watch_silver_step ──┤
    └──► watch_gold_step  ───┘
                             │
                             ▼
                   terminate_emr_cluster
                             │
                             ▼
                            end
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor


# ──────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────
AWS_CONN_ID = "aws_default"

# Airflow Variable `omniroute_config` = contents of config/s3_paths.json
ARTIFACTS = Variable.get("omniroute_config", deserialize_json=True)["artifacts"]

# Uploaded by spark_jobs/streaming/emr_package.sh (path ends in "/")
EMR_ARTIFACTS_PATH = ARTIFACTS["emr"]
BRONZE_SCRIPT    = f"{EMR_ARTIFACTS_PATH}bronze_streaming.py"
SILVER_SCRIPT    = f"{EMR_ARTIFACTS_PATH}silver_streaming.py"
GOLD_SCRIPT      = f"{EMR_ARTIFACTS_PATH}gold_streaming.py"
LIBS_ZIP         = f"{EMR_ARTIFACTS_PATH}omniroute_libs.zip"
BOOTSTRAP_SCRIPT = f"{EMR_ARTIFACTS_PATH}bootstrap_omniroute.sh"

KAFKA_SERVER = "172.31.65.131:9092"

# ──────────────────────────────────────────────────────────────
# EMR Cluster Configuration
# ──────────────────────────────────────────────────────────────
JOB_FLOW_OVERRIDES = {
    "Name": "poc-bootcamp-group5-streaming-cluster",
    "ReleaseLabel": "emr-7.12.0",
    "LogUri": ARTIFACTS["emr_logs"],
    "Applications": [
        {"Name": "Hadoop"},
        {"Name": "Hive"},
        {"Name": "JupyterEnterpriseGateway"},
        {"Name": "Livy"},
        {"Name": "Spark"},
    ],
    "BootstrapActions": [
        {
            "Name": "OmniRoute Bootstrap",
            "ScriptBootstrapAction": {
                "Path": BOOTSTRAP_SCRIPT,
                "Args": [],
            },
        },
    ],
    "Instances": {
        "Ec2KeyName": "group5-project",
        "InstanceGroups": [
            {
                "Name": "Primary",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5a.xlarge",
                "InstanceCount": 1,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {
                            "VolumeSpecification": {
                                "VolumeType": "gp2",
                                "SizeInGB": 32,
                            },
                            "VolumesPerInstance": 2,
                        },
                    ],
                    "EbsOptimized": True,
                },
            },
            {
                "Name": "Core",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5a.xlarge",
                "InstanceCount": 3,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {
                            "VolumeSpecification": {
                                "VolumeType": "gp2",
                                "SizeInGB": 32,
                            },
                            "VolumesPerInstance": 2,
                        },
                    ],
                    "EbsOptimized": True,
                },
            },
            {
                "Name": "Task - 2",
                "Market": "ON_DEMAND",
                "InstanceRole": "TASK",
                "InstanceType": "m5a.xlarge",
                "InstanceCount": 1,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {
                            "VolumeSpecification": {
                                "VolumeType": "gp2",
                                "SizeInGB": 32,
                            },
                            "VolumesPerInstance": 2,
                        },
                    ],
                },
            },
        ],
        "Ec2SubnetId": "{{ var.value.emr_subnet_id }}",
        "EmrManagedMasterSecurityGroup": "sg-0ec16a48a5c4876e3",
        "EmrManagedSlaveSecurityGroup": "sg-064b9227088a5bb70",
        "AdditionalMasterSecurityGroups": ["sg-0ec16a48a5c4876e3"],
        "AdditionalSlaveSecurityGroups": ["sg-064b9227088a5bb70"],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "StepConcurrencyLevel": 3,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "arn:aws:iam::537124955775:role/AmazonEMRServiceRole",
    "EbsRootVolumeSize": 40,
    "ScaleDownBehavior": "TERMINATE_AT_TASK_COMPLETION",
    "VisibleToAllUsers": True,
    "Tags": [
        {"Key": "Project", "Value": "Bootcamp"},
        {"Key": "Environment", "Value": "POC"},
        {"Key": "Owner", "Value": "rahul.pupreja@tothenew.com"},
        {"Key": "CreatedBy", "Value": "aryan.thapliyal@tothenew.com"},
        {"Key": "ManagedBy", "Value": "DataEngineering"},
        {"Key": "Name", "Value": "poc-bootcamp-emr-group5"},
    ],
}


# ──────────────────────────────────────────────────────────────
# EMR Step Definitions (all submitted together → run concurrently)
# ──────────────────────────────────────────────────────────────
STREAMING_STEPS = [
    # ── Step 0: Bronze ──
    {
        "Name": "OmniRoute_Bronze_Streaming",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "client",
                "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
                "--conf", "spark.executor.memory=2g",
                "--conf", "spark.driver.memory=1g",
                "--conf", "spark.executor.cores=2",
                "--conf", "spark.yarn.maxAppAttempts=1",
                "--conf", f"spark.executorEnv.KAFKA_SERVER={KAFKA_SERVER}",
                "--conf", f"spark.driverEnv.KAFKA_SERVER={KAFKA_SERVER}",
                BRONZE_SCRIPT,
            ],
        },
    },
    # ── Step 1: Silver ──
    {
        "Name": "OmniRoute_Silver_Streaming",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "client",
                "--py-files", LIBS_ZIP,
                "--conf", "spark.executor.memory=2g",
                "--conf", "spark.driver.memory=1g",
                "--conf", "spark.executor.cores=2",
                "--conf", "spark.yarn.maxAppAttempts=1",
                SILVER_SCRIPT,
            ],
        },
    },
    # ── Step 2: Gold ──
    {
        "Name": "OmniRoute_Gold_Streaming",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "client",
                "--py-files", LIBS_ZIP,
                "--conf", "spark.executor.memory=2g",
                "--conf", "spark.driver.memory=1g",
                "--conf", "spark.executor.cores=2",
                "--conf", "spark.yarn.maxAppAttempts=1",
                GOLD_SCRIPT,
            ],
        },
    },
]


# ──────────────────────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "omniroute",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="omniroute_emr_streaming_pipeline",
    description=(
        "Creates an EMR cluster and runs Bronze, Silver & Gold "
        "Spark Streaming steps concurrently."
    ),
    schedule=None,                       # Manual trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["emr", "streaming", "bronze", "silver", "gold", "spark", "delta"],
    default_args=default_args,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ══════════════════════════════════════════════════════════
    # CREATE EMR CLUSTER
    # ══════════════════════════════════════════════════════════
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        aws_conn_id=AWS_CONN_ID,
        job_flow_overrides=JOB_FLOW_OVERRIDES,
    )

    # ══════════════════════════════════════════════════════════
    # SUBMIT ALL 3 STEPS CONCURRENTLY
    # ══════════════════════════════════════════════════════════
    add_streaming_steps = EmrAddStepsOperator(
        task_id="add_streaming_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id=AWS_CONN_ID,
        steps=STREAMING_STEPS,
    )

    # ══════════════════════════════════════════════════════════
    # WATCH ALL 3 STEPS IN PARALLEL
    # ══════════════════════════════════════════════════════════
    watch_bronze_step = EmrStepSensor(
        task_id="watch_bronze_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_streaming_steps', key='return_value')[0] }}",
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60,
        timeout=3600,
    )

    watch_silver_step = EmrStepSensor(
        task_id="watch_silver_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_streaming_steps', key='return_value')[1] }}",
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60,
        timeout=3600,
    )

    watch_gold_step = EmrStepSensor(
        task_id="watch_gold_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_streaming_steps', key='return_value')[2] }}",
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60,
        timeout=3600,
    )

    # ══════════════════════════════════════════════════════════
    # TERMINATE EMR CLUSTER
    # ══════════════════════════════════════════════════════════
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id=AWS_CONN_ID,
        trigger_rule="all_done",        # Terminate even if a step fails
    )

    # ══════════════════════════════════════════════════════════
    # TASK DEPENDENCIES
    # ══════════════════════════════════════════════════════════
    start >> create_emr_cluster >> add_streaming_steps

    # Fan out: all 3 sensors run in parallel
    add_streaming_steps >> [watch_bronze_step, watch_silver_step, watch_gold_step]

    # Fan in: terminate only after all 3 finish (or fail)
    [watch_bronze_step, watch_silver_step, watch_gold_step] >> terminate_emr_cluster >> end
