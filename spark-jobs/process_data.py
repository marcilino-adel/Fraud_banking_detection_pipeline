#!/usr/bin/env python3
"""
PySpark job to read data from MinIO input bucket,
perform light processing, and save to output bucket.
"""

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, current_timestamp, lit
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with MinIO S3 configuration"""
    return SparkSession.builder \
        .appName("MinIO Data Processing") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def process_csv_data(spark, input_path, output_path):
    """
    Process CSV data with light transformations
    """
    logger.info(f"Reading data from: {input_path}")

    try:
        # Read CSV data from MinIO input bucket
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(input_path)

        logger.info(f"Input data schema:")
        df.printSchema()
        logger.info(f"Input row count: {df.count()}")

        # Perform light processing
        processed_df = df \
            .withColumn("processed_timestamp", current_timestamp()) \
            .withColumn("processing_job", lit("spark_airflow_job")) \
            .withColumn("row_id", col("id") * 2 if "id" in df.columns else lit(0))

        # Convert string columns to uppercase (if any)
        for col_name in df.columns:
            if df.schema[col_name].dataType.simpleString() == 'string':
                processed_df = processed_df.withColumn(
                    f"{col_name}_upper",
                    upper(col(col_name))
                )

        logger.info(f"Processed data schema:")
        processed_df.printSchema()
        logger.info(f"Processed row count: {processed_df.count()}")

        # Write processed data to MinIO output bucket
        logger.info(f"Writing processed data to: {output_path}")
        processed_df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)

        logger.info("Data processing completed successfully!")
        return True

    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise

def process_json_data(spark, input_path, output_path):
    """
    Process JSON data with light transformations
    """
    logger.info(f"Reading JSON data from: {input_path}")

    try:
        # Read JSON data from MinIO input bucket
        df = spark.read.json(input_path)

        logger.info(f"Input data schema:")
        df.printSchema()
        logger.info(f"Input row count: {df.count()}")

        # Perform light processing
        processed_df = df \
            .withColumn("processed_timestamp", current_timestamp()) \
            .withColumn("processing_job", lit("spark_airflow_job")) \
            .withColumn("record_count", lit(df.count()))

        logger.info(f"Processed data schema:")
        processed_df.printSchema()

        # Write processed data to MinIO output bucket
        logger.info(f"Writing processed data to: {output_path}")
        processed_df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .json(output_path)

        logger.info("JSON data processing completed successfully!")
        return True

    except Exception as e:
        logger.error(f"Error processing JSON data: {str(e)}")
        raise

def main():
    """Main function to execute Spark job"""

    # Get command line arguments
    if len(sys.argv) < 3:
        logger.error("Usage: spark-submit process_data.py <input_path> <output_path> [format]")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    data_format = sys.argv[3] if len(sys.argv) > 3 else "csv"

    logger.info(f"Starting Spark job with parameters:")
    logger.info(f"  Input path: {input_path}")
    logger.info(f"  Output path: {output_path}")
    logger.info(f"  Data format: {data_format}")

    # Create Spark session
    spark = create_spark_session()

    try:
        # Process data based on format
        if data_format.lower() == "json":
            success = process_json_data(spark, input_path, output_path)
        else:
            success = process_csv_data(spark, input_path, output_path)

        if success:
            logger.info("Spark job completed successfully!")
        else:
            logger.error("Spark job failed!")
            sys.exit(1)

    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()