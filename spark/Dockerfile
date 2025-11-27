# Use the Official Apache Spark image (Verified Tag: 3.5.3)
FROM apache/spark:3.5.3

USER root

# 1. Install Python and Pip
# (The official image is minimal, so we ensure Python is ready)
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Install Python Libraries
RUN pip3 install --no-cache-dir \
    boto3 \
    pandas \
    pyarrow \
    numpy

# 3. DOWNLOAD JARS
# Spark 3.5.3 uses Hadoop 3.3.4. We match the JARs exactly.

# --- MinIO / AWS S3 Support ---
RUN curl -o /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
RUN curl -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# --- Snowflake Support ---
RUN curl -o /opt/spark/jars/snowflake-jdbc-3.14.0.jar https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.14.0/snowflake-jdbc-3.14.0.jar
# Use the Spark 3.4 connector (It is compatible with Spark 3.5)
RUN curl -o /opt/spark/jars/spark-snowflake_2.12-2.12.0-spark_3.4.jar https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.12.0-spark_3.4/spark-snowflake_2.12-2.12.0-spark_3.4.jar

# --- Delta Lake Support ---
# Delta 3.0.0 is the correct version for Spark 3.5
RUN curl -o /opt/spark/jars/delta-spark_2.12-3.0.0.jar https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar
RUN curl -o /opt/spark/jars/delta-storage-3.0.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar

# 4. Permissions Fix
# Apache Spark runs as user 185 by default. We must ensure it can write to work directories.
RUN mkdir -p /opt/spark/work-dir && \
    chown -R 185:185 /opt/spark/work-dir && \
    chown -R 185:185 /opt/spark/jars

USER 185