from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import requests

# --- CONFIGURATION ---
SPARK_MASTER_HOST = "spark-master"
SPARK_PORT = "6066"
SCRIPT_PATH = "/opt/spark-jobs/process_silver.py"


def submit_spark_job(**context):
    # 1. GET CONFIGURATION (This is where the step comes from)
    step = context['dag_run'].conf.get('step', 1)
    print(f"--- Submitting Spark Job for Step: {step} ---")

    url = f"http://{SPARK_MASTER_HOST}:{SPARK_PORT}/v1/submissions/create"

    payload = {
        "action": "CreateSubmissionRequest",

        # Args: [Script Path, Step Number]
        "appArgs": [SCRIPT_PATH, str(step)],

        "appResource": f"file:{SCRIPT_PATH}",
        "clientSparkVersion": "3.5.3",
        "mainClass": "org.apache.spark.deploy.PythonRunner",
        "environmentVariables": {
            "SPARK_ENV_LOADED": "1"
        },
        "sparkProperties": {
            "spark.master": "spark://spark-master:7077",
            "spark.driver.supervise": "false",
            "spark.app.name": f"Silver_Processing_Step_{step}",
            "spark.submit.deployMode": "cluster",

            # --- APPLYING THE FIXES FROM GOLD DAG ---
            # 1. Ensure Delta/Hadoop JARs are found
            "spark.driver.extraClassPath": "/opt/spark/jars/*",
            "spark.executor.extraClassPath": "/opt/spark/jars/*",

            # 2. Prevent Memory Deadlock (Small Driver, Medium Worker)
            "spark.driver.memory": "512m",
            "spark.executor.memory": "1g",
            "spark.cores.max": "1"
        }
    }

    print(f"Sending request to: {url}")

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

with DAG('03_process_silver',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:
    submit_task = PythonOperator(
        task_id='submit_silver_processing',
        python_callable=submit_spark_job,
        provide_context=True
    )