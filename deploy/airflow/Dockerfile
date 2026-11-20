FROM apache/airflow:2.10.5

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends procps \
 && rm -rf /var/lib/apt/lists/*

# JDK 21 (Temurin) runtime, per the Prerequisites Guide
COPY --from=eclipse-temurin:21-jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:${PATH}" \
    SPARK_CONF_DIR=/opt/spark-conf

# Every SparkSession gets the Kafka connector + Postgres JDBC from a pre-filled cache
RUN mkdir -p /opt/spark-conf /opt/spark-ivy \
 && printf '%s\n' \
    'spark.jars.packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.4,org.postgresql:postgresql:42.7.4' \
    'spark.jars.ivy /opt/spark-ivy' \
    > /opt/spark-conf/spark-defaults.conf \
 && chown -R airflow:0 /opt/spark-conf /opt/spark-ivy \
 && chmod -R g+rwX /opt/spark-ivy

USER airflow

# Airflow-compatible packages, pinned by the official constraints file (image uses Python 3.12)
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt" \
    "apache-airflow==${AIRFLOW_VERSION}" \
    psycopg2-binary \
    pyyaml \
    python-dotenv \
    requests

# PySpark separately, so the constraints file can't hold it back
RUN pip install --no-cache-dir pyspark==4.1.4

# Download the connector jars at build time, not at runtime
RUN python -c "from pyspark.sql import SparkSession; SparkSession.builder.master('local[1]').getOrCreate().stop()"