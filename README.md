# Fraud Banking Detection Pipeline

A fraud detection data pipeline for banking transactions built with a **Medallion Architecture** (Bronze → Silver → Gold). It ingests simulated multi-source transaction data, processes it through Apache Spark with Delta Lake, and exports business-ready analytics to Snowflake.

## Architecture

```
                        ┌──────────────┐
                        │  Raw CSV     │
                        │  (6M+ rows)  │
                        └──────┬───────┘
                               │
                     simulate_split.py
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                 ▼
     source_transactions  source_accounts  source_fraud_labels
              │                │                 │
              └────────────────┼─────────────────┘
                               │
                   Airflow DAG: Bronze Layer
                   (02_ingest_hourly_batch)
                               │
                               ▼
                     ┌─────────────────┐
                     │  MinIO (S3)     │
                     │  raw-data/      │
                     └────────┬────────┘
                              │
                  Airflow DAG: Silver Layer
                  (03_process_silver → Spark)
                              │
                              ▼
                    ┌──────────────────┐
                    │  MinIO (S3)      │
                    │  processed-data/ │
                    │  (Delta Lake)    │
                    └────────┬─────────┘
                             │
                 Airflow DAG: Gold Layer
                 (04_process_gold → Spark)
                             │
                             ▼
                    ┌─────────────────┐
                    │   Snowflake     │
                    │   (4 tables)    │
                    └─────────────────┘
```

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Orchestration | Apache Airflow 2.6.0 | DAG scheduling & workflow management |
| Processing | Apache Spark 3.5.3 | Distributed data processing |
| Storage (Bronze/Silver) | MinIO | S3-compatible object storage |
| Storage (Gold) | Snowflake | Cloud data warehouse |
| Table Format | Delta Lake 3.0.0 | ACID transactions on the Silver layer |
| Metadata DB | PostgreSQL 13 | Airflow backend |
| Containerization | Docker & Docker Compose | Full stack orchestration |

## Project Structure

```
Fraud_banking_detection_pipeline/
├── dags/
│   ├── 02_ingest_hourly_batch.py      # Bronze: upload hourly CSV batches to MinIO
│   ├── 03_process_silver.py           # Silver: join, clean, enrich via Spark
│   ├── 04_process_gold.py             # Gold: compute analytics, write to Snowflake
│   └── spark_minio_processing_dag.py  # Generic Spark + MinIO test DAG
├── spark-jobs/
│   ├── process_data.py                # Generic CSV/JSON Spark processor
│   ├── process_silver.py              # Silver layer transformation logic
│   ├── process_gold.py                # Gold layer metrics & Snowflake export
│   └── inspect_silver.py             # Delta Lake inspection utility
├── spark/
│   └── Dockerfile                     # Spark image with Delta Lake & Snowflake JARs
├── scripts/
│   └── simulate_split.py             # Splits raw dataset into hourly batches
├── docker-compose.yml                 # Full infrastructure definition
└── LICENSE
```

## Pipeline Layers

### Bronze (Raw Ingestion)
- Reads simulated hourly batches from three source systems (transactions, accounts, fraud labels)
- Uploads raw CSVs to MinIO bucket `raw-data`

### Silver (Cleaned & Enriched)
- Spark joins the three sources by `transaction_id`
- Adds computed fields: `transaction_time` (timestamp from step), `is_corrupt` (balance integrity flag)
- Stored as Delta Lake format in MinIO bucket `processed-data`

### Gold (Business Analytics)
- Reads the Silver Delta table and computes four analytical tables written to Snowflake:
  - **HOURLY_FRAUD_METRICS** — aggregated fraud counts, money moved, averages by hour & type
  - **VICTIM_PROFILE** — average balances & transaction sizes by fraud status
  - **SYSTEM_AUDIT** — confusion matrix (missed fraud, caught fraud, false alarms)
  - **TRANSACTION_TRENDS_DETAILED** — individual transactions with status labels

## Dependencies

### Python Packages

| Package | Purpose |
|---------|---------|
| `pandas` | Data manipulation (simulate_split, DAGs) |
| `boto3` | MinIO/S3 client for Bronze layer uploads |
| `pyarrow` | Arrow format support for Spark |
| `numpy` | Numerical operations in data simulation |
| `pyspark` | Spark Python API |
| `requests` | HTTP client for Spark REST API submissions |

### JVM Dependencies (bundled in Spark Docker image)

| JAR | Purpose |
|-----|---------|
| `hadoop-aws-3.3.4` | S3/MinIO filesystem support |
| `aws-java-sdk-bundle-1.12.262` | AWS SDK for S3 operations |
| `snowflake-jdbc-3.14.0` | Snowflake JDBC driver |
| `spark-snowflake_2.12-2.12.0-spark_3.4` | Spark-Snowflake connector |
| `delta-spark_2.12-3.0.0` | Delta Lake core |
| `delta-storage-3.0.0` | Delta Lake storage layer |

## Prerequisites

- **Docker** and **Docker Compose** installed
- **Fraud dataset CSV** — a CSV file with 6M+ rows containing columns: `type`, `amount`, `nameOrig`, `nameDest`, `oldbalanceOrg`, `newbalanceOrig`, `oldbalanceDest`, `newbalanceDest`, `isFraud`, `isFlaggedFraud`, `step`
- **Snowflake account** with credentials (for the Gold layer)

## How to Run

### 1. Start the Infrastructure

```bash
docker-compose up -d
```

Wait for all services to become healthy. Access the UIs to verify:

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Airflow Web UI | http://localhost:8085 | `admin` / `admin` |
| Spark Master UI | http://localhost:8080 | — |

### 2. Configure Snowflake Credentials

Edit `spark-jobs/process_gold.py` and update the Snowflake connection options:

```python
sf_options = {
    "sfURL": "<your-account>.snowflakecomputing.com",
    "sfUser": "<your-user>",
    "sfPassword": "<your-password>",
    "sfDatabase": "<your-database>",
    "sfSchema": "<your-schema>",
    "sfWarehouse": "<your-warehouse>",
}
```

### 3. Prepare the Data

Place your fraud dataset CSV at `data/fraud_data.csv`, then run:

```bash
cd scripts
python simulate_split.py
```

This splits the dataset into hourly batches across three source directories:
- `data/source_transactions/batch_step_X.csv`
- `data/source_account_info/batch_step_X.csv`
- `data/source_fraud_labels/batch_step_X.csv`

### 4. Run the Pipeline

Trigger each layer sequentially via the Airflow UI or API:

**Bronze — Ingest a batch:**
```bash
curl -X POST http://localhost:8085/api/v1/dags/02_ingest_hourly_batch/dagRuns \
  -H "Content-Type: application/json" \
  -u admin:admin \
  -d '{"conf": {"step": 1}}'
```

**Silver — Process the ingested batch:**
```bash
curl -X POST http://localhost:8085/api/v1/dags/03_process_silver/dagRuns \
  -H "Content-Type: application/json" \
  -u admin:admin \
  -d '{"conf": {"step": 1}}'
```

**Gold — Generate analytics and export to Snowflake:**
```bash
curl -X POST http://localhost:8085/api/v1/dags/04_process_gold/dagRuns \
  -H "Content-Type: application/json" \
  -u admin:admin \
  -d '{}'
```

Repeat Bronze and Silver for additional batches (`step: 2`, `step: 3`, etc.). The Gold layer processes all accumulated Silver data.

### 5. View Results

Query the four tables in your Snowflake warehouse:
- `HOURLY_FRAUD_METRICS`
- `VICTIM_PROFILE`
- `SYSTEM_AUDIT`
- `TRANSACTION_TRENDS_DETAILED`

## Docker Services

| Service | Image | Ports |
|---------|-------|-------|
| `minio` | `minio/minio:latest` | 9000 (API), 9001 (Console) |
| `minio-init` | `minio/mc:latest` | Creates buckets on startup |
| `spark-master` | Custom (see `spark/Dockerfile`) | 7077, 8080, 6066 |
| `spark-worker` | Custom (see `spark/Dockerfile`) | 8081 |
| `postgres` | `postgres:13` | 5432 (internal) |
| `airflow-webserver` | `apache/airflow:2.6.0` | 8085 |
| `airflow-scheduler` | `apache/airflow:2.6.0` | — |

## Stopping the Pipeline

```bash
docker-compose down
```

To also remove stored data volumes:

```bash
docker-compose down -v
```

## License

MIT
