from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit, expr
import sys

# --- CONFIGURATION ---
MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "raw-data"
SILVER_BUCKET = "processed-data"


def create_spark_session():
    """Creates a Spark Session with Delta Lake & MinIO support."""
    return SparkSession.builder \
        .appName("SilverLayerProcessing") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def process_batch(step):
    spark = create_spark_session()
    print(f"--- STARTING SILVER PROCESSING FOR STEP {step} ---")

    # 1. READ BRONZE (Separate Systems)
    # We read directly from S3 (MinIO)
    path_trans = f"s3a://{BUCKET_NAME}/transactions/batch_step_{step}.csv"
    path_acct = f"s3a://{BUCKET_NAME}/accounts/batch_step_{step}.csv"
    path_fraud = f"s3a://{BUCKET_NAME}/fraud/batch_step_{step}.csv"

    print("Reading data from MinIO...")
    df_trans = spark.read.option("header", "true").option("inferSchema", "true").csv(path_trans)
    df_acct = spark.read.option("header", "true").option("inferSchema", "true").csv(path_acct)
    df_fraud = spark.read.option("header", "true").option("inferSchema", "true").csv(path_fraud)

    # 2. JOIN SYSTEMS (Reconstructing the Reality)
    # We use 'transaction_id' to stitch the microservices back together
    df_joined = df_trans.join(df_acct, on="transaction_id", how="inner") \
        .join(df_fraud, on="transaction_id", how="inner")

    # 3. TRANSFORMATIONS
    # A. Convert Step to Timestamp + Randomize Minutes/Seconds
    # Logic: Start Date + (Step * Hours) + (Random Seconds between 0 and 3600)
    # This spreads the transactions out over the full hour so they look real.
    df_enriched = df_joined.withColumn("transaction_time",
                                       expr(
                                           "timestamp('2025-01-01') + make_interval(0, 0, 0, 0, step, 0, 0) + make_interval(0, 0, 0, 0, 0, 0, rand() * 3600)")
                                       )

    # B. Data Quality Logic: "The Math Check"
    # Does Old - Amount = New? (Allowing for tiny float errors)
    # We add a flag 'is_corrupt' so we don't delete data, just mark it.
    df_enriched = df_enriched.withColumn("is_corrupt",
                                         expr("abs((oldbalanceOrg - amount) - newbalanceOrig) > 0.01"))

    # 4. WRITE TO SILVER (Delta Lake)
    # We append to the main Silver Table
    silver_path = f"s3a://{SILVER_BUCKET}/transactions_silver"

    print(f"Writing {df_enriched.count()} rows to Silver Layer (Delta)...")
    df_enriched.write.format("delta").mode("append").save(silver_path)

    print("✅ Silver Layer Processing Complete!")
    spark.stop()


if __name__ == "__main__":
    # We allow passing the Step number as a command line argument
    if len(sys.argv) > 1:
        step_arg = sys.argv[1]
    else:
        step_arg = 1  # Default

    process_batch(step_arg)