"""
Airflow DAG to trigger Spark jobs for processing data between MinIO buckets.
MinIO and Spark run as external services on the host network.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import logging
import subprocess
import os

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'spark_minio_processing',
    default_args=default_args,
    description='Process data from MinIO input bucket to output bucket using Spark',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['spark', 'minio', 'etl'],
)

def check_minio_connection(**context):
    """Check if MinIO is accessible"""
    import boto3
    from botocore.client import Config

    try:
        # Connect to MinIO on host machine
        s3_client = boto3.client(
            's3',
            endpoint_url='http://host.docker.internal:9000',  # Access host from container
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )

        # List buckets to verify connection
        response = s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        logging.info(f"Available MinIO buckets: {buckets}")

        # Check if our buckets exist
        required_buckets = ['input-bucket', 'output-bucket']
        for bucket in required_buckets:
            if bucket not in buckets:
                logging.warning(f"Bucket {bucket} not found. Please ensure MinIO is properly initialized.")
                return False

        logging.info("MinIO connection successful!")
        return True
    except Exception as e:
        logging.error(f"MinIO connection failed: {str(e)}")
        logging.error("Make sure MinIO is running: docker-compose -f docker-compose.yml up -d")
        raise

def upload_sample_data(**context):
    """Upload sample data to MinIO input bucket for testing"""
    import boto3
    from botocore.client import Config
    import pandas as pd
    import io

    try:
        # Create S3 client for MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url='http://host.docker.internal:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )

        # Create sample CSV data
        sample_data = pd.DataFrame({
            'id': range(1, 101),
            'name': [f'Customer_{i}' for i in range(1, 101)],
            'age': [20 + (i % 50) for i in range(100)],
            'city': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'] * 20,
            'purchase_amount': [100.0 + (i * 10.5) for i in range(100)],
            'purchase_date': pd.date_range(start='2024-01-01', periods=100, freq='D').strftime('%Y-%m-%d').tolist(),
            'category': ['Electronics', 'Clothing', 'Food', 'Books', 'Sports'] * 20,
            'score': [70 + (i % 30) for i in range(100)]
        })

        # Convert DataFrame to CSV string
        csv_buffer = io.StringIO()
        sample_data.to_csv(csv_buffer, index=False)
        csv_string = csv_buffer.getvalue()

        # Upload to MinIO
        file_name = f"sample_data_{context['ds']}.csv"
        s3_client.put_object(
            Bucket='input-bucket',
            Key=file_name,
            Body=csv_string.encode('utf-8')
        )

        logging.info(f"Sample data uploaded: {file_name}")
        logging.info(f"Data shape: {sample_data.shape}")
        logging.info(f"Columns: {list(sample_data.columns)}")

        # Store filename in XCom for downstream tasks
        context['task_instance'].xcom_push(key='input_file', value=file_name)
        return file_name

    except Exception as e:
        logging.error(f"Failed to upload sample data: {str(e)}")
        raise

def submit_spark_job(**context):
    """Submit Spark job to external Spark cluster"""
    import subprocess

    try:
        # Get input filename from XCom
        input_file = context['task_instance'].xcom_pull(task_ids='upload_sample_data', key='input_file')

        # Build spark-submit command to run on external Spark cluster
        spark_submit_cmd = [
            "docker", "exec", "spark-master",
            "spark-submit",
            "--master", "spark://spark-master:7077",
            "--deploy-mode", "client",
            "--conf", "spark.hadoop.fs.s3a.endpoint=http://minio:9000",
            "--conf", "spark.hadoop.fs.s3a.access.key=minioadmin",
            "--conf", "spark.hadoop.fs.s3a.secret.key=minioadmin",
            "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
            "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
            "/opt/spark-jobs/process_data.py",
            f"s3a://input-bucket/{input_file}",
            f"s3a://output-bucket/processed_{context['ds']}",
            "csv"
        ]

        # Execute the command
        logging.info(f"Executing Spark job: {' '.join(spark_submit_cmd)}")
        result = subprocess.run(spark_submit_cmd, capture_output=True, text=True, check=True)

        logging.info("Spark job output:")
        logging.info(result.stdout)

        if result.stderr:
            logging.warning("Spark job stderr:")
            logging.warning(result.stderr)

        return True

    except subprocess.CalledProcessError as e:
        logging.error(f"Spark job failed with exit code {e.returncode}")
        logging.error(f"stdout: {e.stdout}")
        logging.error(f"stderr: {e.stderr}")
        raise
    except Exception as e:
        logging.error(f"Failed to submit Spark job: {str(e)}")
        raise

def verify_output(**context):
    """Verify the output data in MinIO output bucket"""
    import boto3
    from botocore.client import Config

    try:
        s3_client = boto3.client(
            's3',
            endpoint_url='http://host.docker.internal:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )

        # List objects in output bucket
        prefix = f"processed_{context['ds']}"
        response = s3_client.list_objects_v2(Bucket='output-bucket', Prefix=prefix)

        if 'Contents' in response:
            logging.info(f"Output files found for prefix '{prefix}':")
            total_size = 0
            for obj in response['Contents']:
                logging.info(f"  - {obj['Key']} (Size: {obj['Size']} bytes, Modified: {obj['LastModified']})")
                total_size += obj['Size']

            logging.info(f"Total output size: {total_size} bytes")

            # Optionally, read and display a sample of the output
            if response['Contents']:
                first_file = response['Contents'][0]['Key']
                obj = s3_client.get_object(Bucket='output-bucket', Key=first_file)
                content = obj['Body'].read().decode('utf-8')
                lines = content.split('\n')[:5]  # First 5 lines
                logging.info("Sample output data:")
                for line in lines:
                    logging.info(f"  {line}")

            return True
        else:
            logging.warning(f"No output files found in output bucket with prefix '{prefix}'")
            return False

    except Exception as e:
        logging.error(f"Failed to verify output: {str(e)}")
        raise

def cleanup_old_data(**context):
    """Optional: Clean up old data from buckets"""
    import boto3
    from botocore.client import Config
    from datetime import datetime, timedelta

    try:
        s3_client = boto3.client(
            's3',
            endpoint_url='http://host.docker.internal:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )

        # Calculate cutoff date (e.g., 7 days ago)
        cutoff_date = datetime.now() - timedelta(days=7)

        for bucket in ['input-bucket', 'output-bucket']:
            response = s3_client.list_objects_v2(Bucket=bucket)

            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['LastModified'].replace(tzinfo=None) < cutoff_date:
                        logging.info(f"Deleting old file: {bucket}/{obj['Key']}")
                        s3_client.delete_object(Bucket=bucket, Key=obj['Key'])

        logging.info("Cleanup completed")
        return True

    except Exception as e:
        logging.warning(f"Cleanup failed (non-critical): {str(e)}")
        return False

# Task 1: Check MinIO Connection
check_minio_task = PythonOperator(
    task_id='check_minio_connection',
    python_callable=check_minio_connection,
    dag=dag,
)

# Task 2: Upload Sample Data
upload_sample_data_task = PythonOperator(
    task_id='upload_sample_data',
    python_callable=upload_sample_data,
    dag=dag,
)

# Task 3: Submit Spark Job
submit_spark_task = PythonOperator(
    task_id='submit_spark_job',
    python_callable=submit_spark_job,
    dag=dag,
)

# Task 4: Verify Output
verify_output_task = PythonOperator(
    task_id='verify_output',
    python_callable=verify_output,
    dag=dag,
)

# Task 5: Optional Cleanup (can be enabled/disabled)
cleanup_task = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_data,
    dag=dag,
    trigger_rule='none_failed',  # Run even if previous tasks are skipped
)

# Define task dependencies
check_minio_task >> upload_sample_data_task >> submit_spark_task >> verify_output_task
# Uncomment to enable cleanup:
# verify_output_task >> cleanup_task