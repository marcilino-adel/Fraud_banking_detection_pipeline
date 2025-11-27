from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import requests

# --- CONFIGURATION ---
SPARK_MASTER_HOST = "spark-master"
SPARK_PORT = "6066"
SCRIPT_PATH = "/opt/spark-jobs/process_gold.py"


def submit_gold_job(**context):
    print("--- Submitting Gold Layer Job (Snowflake) ---")

    url = f"http://{SPARK_MASTER_HOST}:{SPARK_PORT}/v1/submissions/create"

    payload = {
        "action": "CreateSubmissionRequest",
        # FIXED: PythonRunner needs [Script, PyFiles, Args...]
        "appArgs": [SCRIPT_PATH, ""],
        "appResource": f"file:{SCRIPT_PATH}",
        "clientSparkVersion": "3.5.3",
        "mainClass": "org.apache.spark.deploy.PythonRunner",
        "environmentVariables": {
            "SPARK_ENV_LOADED": "1"
        },
        "sparkProperties": {
            "spark.master": "spark://spark-master:7077",
            "spark.driver.supervise": "false",
            "spark.app.name": "Gold_Layer_Snowflake",
            "spark.submit.deployMode": "cluster",

            # Ensure JARs are found
            "spark.driver.extraClassPath": "/opt/spark/jars/*",
            "spark.executor.extraClassPath": "/opt/spark/jars/*",

            # FIX: Reduce Driver Memory to prevent Deadlock
            "spark.driver.memory": "512m",   # The Manager (Small)
            "spark.executor.memory": "1g",   # The Worker (Big)
            "spark.cores.max": "1"           # Limit to 1 core so we don't block other tasks
        }
    }

    try:
        response = requests.post(
            url,
            headers={'Content-Type': 'application/json;charset=UTF-8'},
            data=json.dumps(payload)
        )
        print(f"Response: {response.text}")

        if response.status_code != 200:
            raise Exception(f"Spark Error: {response.status_code}")

        data = response.json()
        if not data.get('success'):
            raise Exception(f"Spark Submission Failed: {data}")

        print(f"✅ Job Submitted! ID: {data.get('submissionId')}")

    except Exception as e:
        print(f"❌ Connection Error: {e}")
        raise e


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

with DAG('04_process_gold',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:
    submit_task = PythonOperator(
        task_id='submit_gold_processing',
        python_callable=submit_gold_job,
        provide_context=True
    )