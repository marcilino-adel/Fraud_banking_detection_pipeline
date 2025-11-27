from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import boto3
from botocore.client import Config

# --- CONFIGURATION ---
MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "raw-data"

# Docker Paths (Where Airflow sees the data)
BASE_PATH = "/opt/airflow/data"


def upload_system_batch(**context):
    """
    Reads the 'step' from the DAG configuration and uploads
    the 3 files for that specific hour.
    """
    # 1. Get the Step/Hour from the Trigger Configuration (Default to 1 if not provided)
    step = context['dag_run'].conf.get('step', 1)
    print(f"--- STARTING INGESTION FOR BATCH STEP: {step} ---")

    # 2. Define the 3 Systems and their paths
    systems = {
        "transactions": f"source_transactions/batch_step_{step}.csv",
        "accounts": f"source_account_info/batch_step_{step}.csv",
        "fraud": f"source_fraud_labels/batch_step_{step}.csv"
    }

    # 3. Connect to MinIO
    s3 = boto3.client('s3',
                      endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY,
                      config=Config(signature_version='s3v4'))

    # 4. Loop through systems and upload
    for system_name, relative_path in systems.items():
        local_file = os.path.join(BASE_PATH, relative_path)
        minio_path = f"{system_name}/batch_step_{step}.csv"  # e.g., transactions/batch_step_1.csv

        # Check if file exists
        if not os.path.exists(local_file):
            raise FileNotFoundError(
                f"CRITICAL: Simulation file missing at {local_file}. Did you run simulate_split.py?")

        # Upload
        print(f"Uploading {system_name.upper()} data to {BUCKET_NAME}/{minio_path}...")
        s3.upload_file(local_file, BUCKET_NAME, minio_path)

    print(f"✅ Batch {step} successfully ingested into Bronze Layer.")


# --- DAG DEFINITION ---
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

with DAG('02_ingest_hourly_batch',
         default_args=default_args,
         schedule_interval=None,  # We trigger this manually or via API
         catchup=False) as dag:
    ingest_task = PythonOperator(
        task_id='ingest_batch_to_minio',
        python_callable=upload_system_batch,
        provide_context=True
    )