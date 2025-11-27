from pyspark.sql import SparkSession

# --- CONFIGURATION ---
MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
SILVER_PATH = "s3a://processed-data/transactions_silver"


def inspect_data():
    spark = SparkSession.builder \
        .appName("InspectSilver") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    print(f"Reading Delta Table from {SILVER_PATH}...")

    # Read the data as a Delta Table
    df = spark.read.format("delta").load(SILVER_PATH)

    # 1. Show Schema (Did we get the Timestamp?)
    print("\n--- SCHEMA ---")
    df.printSchema()

    # 2. Run a SQL Query (Check for Corruption)
    print("\n--- DATA PREVIEW (Top 20 Rows) ---")
    df.show(20, truncate=False)

    # 3. Check Row Count
    count = df.count()
    print(f"\n--- TOTAL ROWS: {count} ---")

    spark.stop()


if __name__ == "__main__":
    inspect_data()