# Airflow image that can also run the PySpark Pre-DQ job (src/dq/pre_dq_checks.py) in local mode.
FROM eclipse-temurin:21.0.12.1_1-jre AS jre

FROM apache/airflow:2.10.5

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends procps \
 && rm -rf /var/lib/apt/lists/*

# JDK 21 (Temurin) runtime, per the Prerequisites Guide
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:${PATH}"

USER airflow

# Pin apache-airflow so pip can't upgrade it
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    pyspark==4.1.4 \
    python-dotenv \
    pandas \
    numpy \
    scikit-learn \
    psycopg2-binary \
    kafka-python-ng \
    pyyaml \
    skl2onnx \
    onnx \
    onnxruntime \
    google-cloud-pubsub \
    requests

# Kafka connector (+ its runtime deps) and Postgres JDBC, placed directly on PySpark's classpath
# at build time. Nothing is downloaded when the job starts, so no Ivy/Maven access is needed at
# runtime (a failed runtime download is what kills the JVM with JAVA_GATEWAY_EXITED).
# Versions match pyspark 4.1.4 (spark-parent pom: kafka.version 3.9.1, commons-pool2 2.12.1).
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eu; \
    JARS_DIR="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"; \
    cd "$JARS_DIR"; \
    fetch() { curl -fsSL -o "$(basename "$1")" "$MAVEN/$1" && echo "$2  $(basename "$1")" | sha256sum -c -; }; \
    fetch org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.4/spark-sql-kafka-0-10_2.13-4.1.4.jar 94f9514f64b319aa31c3b2061bf2fb75700f82e14845fda71337a13696855710; \
    fetch org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.4/spark-token-provider-kafka-0-10_2.13-4.1.4.jar b641257bf720279865206a9cd8d69691c8004bc18e653829c6e1aa1d748c6716; \
    fetch org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar 7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9; \
    fetch org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar 5273c8bc8c0dca2211175c0d27efbd7228efae99689aac001a8e1ef8e872e9ef; \
    fetch org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar 73914527305a40cce504b0d3d90b23caf565136912d607ae8ae7c5895512332c

# Tell src/dq/pre_dq_checks.py not to request spark.jars.packages: the jars are already baked in.
ENV SPARK_JARS_PACKAGES=""

# Build-time smoke test: Java starts, Spark starts, and the Kafka source class is on the classpath.
RUN python -c "\
from pyspark.sql import SparkSession; \
s = SparkSession.builder.master('local[1]').getOrCreate(); \
s._jvm.java.lang.Class.forName('org.apache.spark.sql.kafka010.KafkaSourceProvider'); \
print('spark', s.version, 'with kafka connector OK'); \
s.stop()"
