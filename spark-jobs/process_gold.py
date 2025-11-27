from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as _sum, avg, col, lit, when

# --- CONFIGURATION ---
# Source (MinIO)
MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
SILVER_PATH = "s3a://processed-data/transactions_silver"

# Destination (Snowflake)
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
sfOptions = {
    # UPDATE THESE WITH YOUR DETAILS
    "sfUrl": "hyqkdbd-be57374.snowflakecomputing.com",
    "sfUser": "Marcilino",
    "sfPassword": "Snowflake@j123",  # <--- UPDATE THIS
    "sfDatabase": "FRAUD_DB",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "dbtable": "HOURLY_FRAUD_METRICS"  # <--- ADD THIS HERE
}


def process_gold():
    # 1. Initialize Spark
    # We explicitly list every JAR we need to ensure they are on the Classpath
    jars = [
        "/opt/spark/jars/snowflake-jdbc-3.14.0.jar",
        "/opt/spark/jars/spark-snowflake_2.12-2.12.0-spark_3.4.jar",
        "/opt/spark/jars/delta-spark_2.12-3.0.0.jar",
        "/opt/spark/jars/delta-storage-3.0.0.jar",
        "/opt/spark/jars/hadoop-aws-3.3.4.jar",
        "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
    ]

    spark = SparkSession.builder \
        .appName("GoldLayerSnowflake") \
        .config("spark.jars", ",".join(jars)) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    print("Reading Silver Delta Table from MinIO...")
    try:
        df_silver = spark.read.format("delta").load(SILVER_PATH)
        # This loads the data into RAM once, so we don't re-read MinIO for every table
        df_silver.cache()
        print(f"Cached {df_silver.count()} rows in memory.")
    except Exception as e:
        print("❌ Error: Could not read Silver Data.")
        raise e

    # 2. BUSINESS LOGIC
    print("Calculating Gold Metrics...")
    gold_df = df_silver.groupBy("transaction_time", "type") \
        .agg(
        count("transaction_id").alias("total_transactions"),
        _sum("isFraud").alias("fraud_cases"),
        _sum("amount").alias("total_money_moved"),
        avg("amount").alias("average_txn_amount")
    ) \
        .orderBy("transaction_time")

    # 3. WRITE TO SNOWFLAKE
    print("Pushing Gold Table to Snowflake...")

    gold_df.write \
        .format(SNOWFLAKE_SOURCE_NAME) \
        .options(**sfOptions) \
        .mode("overwrite") \
        .save("")

    print("✅ Gold Layer processing complete! Data is in Snowflake.")
    # ... (Previous code writing HOURLY_FRAUD_METRICS is above this) ...

    # --- NEW: ANSWERING QUESTION C (Victim Profile) ---
    print("Calculating Victim Profile (High vs Low Balance)...")

    # Logic: We want to see the average balance of people who got defrauded vs safe people
    # We also categorize them by Transaction Type to see if vectors change based on wealth
    victim_df = df_silver.groupBy("type", "isFraud") \
        .agg(
        count("transaction_id").alias("total_count"),
        avg("oldbalanceOrg").alias("avg_victim_balance"),
        avg("amount").alias("avg_transaction_size")
    ) \
        .orderBy("type", "isFraud")

    # Write Table 2: VICTIM_PROFILE
    print("Pushing Victim Profile to Snowflake...")
    victim_df.write \
        .format(SNOWFLAKE_SOURCE_NAME) \
        .options(**sfOptions) \
        .option("dbtable", "VICTIM_PROFILE") \
        .mode("overwrite") \
        .save()

    # --- NEW: ANSWERING QUESTION D (System Efficiency) ---
    print("Calculating System Efficiency (Confusion Matrix)...")

    # Logic: Compare the Machine's Guess (isFlaggedFraud) vs Reality (isFraud)
    system_audit_df = df_silver.groupBy("type") \
        .agg(
        # 1. False Negatives: System slept, Money stolen (The worst case)
        _sum(col("isFraud") * (1 - col("isFlaggedFraud"))).alias("missed_fraud"),

        # 2. True Positives: System caught it
        _sum(col("isFraud") * col("isFlaggedFraud")).alias("caught_fraud"),

        # 3. False Positives: System blocked innocent person
        _sum((1 - col("isFraud")) * col("isFlaggedFraud")).alias("false_alarms")
    )

    # Write Table 3: SYSTEM_AUDIT
    print("Pushing System Audit to Snowflake...")
    system_audit_df.write \
        .format(SNOWFLAKE_SOURCE_NAME) \
        .options(**sfOptions) \
        .option("dbtable", "SYSTEM_AUDIT") \
        .mode("overwrite") \
        .save()

    # --- NEW TABLE 4: DETAILED TRENDS (For Rich Visuals) ---
    # This table keeps individual rows but cleans columns for plotting
    print("Calculating 4/4: Detailed Trends (Scatter Plot Data)...")

    detailed_df = df_silver.select(
        col("transaction_time"),
        col("type"),
        col("amount"),
        col("oldbalanceOrg"),
        col("newbalanceOrig"),
        # Create a readable Label for visuals instead of 0/1
        when(col("isFraud") == 1, "FRAUD").otherwise("LEGIT").alias("status"),
        # Calculate the error in the transaction math (Data Quality metric)
        (col("oldbalanceOrg") - col("amount") - col("newbalanceOrig")).alias("math_error")
    )

    detailed_df.write \
        .format(SNOWFLAKE_SOURCE_NAME) \
        .options(**sfOptions) \
        .option("dbtable", "TRANSACTION_TRENDS_DETAILED") \
        .mode("overwrite") \
        .save()

    print("✅ All 4 Gold Tables successfully pushed to Snowflake.")


    spark.stop()


if __name__ == "__main__":
    process_gold()