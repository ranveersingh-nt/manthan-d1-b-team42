"""Bronze loader: copies new rows from payment_events_validated into payment_events_bronze.

Every BRONZE_POLL_SECONDS it:
  1. asks bronze how far it got:     SELECT max(ingested_at) FROM payment_events_bronze
  2. reads newer validated rows      (Spark JDBC, with a small overlap for late commits)
  3. adds bronze_loaded_at           (one value for the whole run)
  4. writes them                     INSERT ... ON CONFLICT (event_id) DO NOTHING

The overlap re-reads a few rows each run; ON CONFLICT makes that harmless, so the job can
stop and restart at any time without losing or duplicating rows.

Run:  python -m src.processor.bronze            (loop forever, Ctrl+C to stop)
      python -m src.processor.bronze --once     (one run, e.g. from Airflow or a test)
"""

import argparse
import logging
import os
import re
import time
from datetime import timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.processor.sinks import pg_connect, write_rows

log = logging.getLogger("bronze")

SOURCE_TABLE = "payment_events_validated"
POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"

SOURCE_COLUMNS = [
    "event_id", "card_token", "merchant_id", "merchant_category", "amount", "currency",
    "country", "channel", "event_ts", "status", "schema_version",
    "src_topic", "src_partition", "src_offset", "src_kafka_ts", "ingested_at",
]
BRONZE_COLUMNS = SOURCE_COLUMNS + ["bronze_loaded_at"]


def bronze_table():
    """Table name from env. Checked because it is put into SQL text (it can't be a parameter)."""
    name = os.getenv("BRONZE_TABLE", "payment_events_bronze")
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", name):
        raise ValueError(f"BRONZE_TABLE must be a plain lowercase table name, got {name!r}")
    return name


def source_query(since, overlap_seconds):
    """SQL for the validated rows to copy. since=None (empty bronze) means copy everything."""
    sql = f"SELECT {', '.join(SOURCE_COLUMNS)} FROM {SOURCE_TABLE}"
    if since is None:
        return sql
    start = since - timedelta(seconds=overlap_seconds)
    return f"{sql} WHERE ingested_at > '{start.isoformat()}'::timestamptz"


def read_resume_point(table):
    """Latest ingested_at already in bronze, or None if bronze is empty."""
    conn = pg_connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f"SELECT max(ingested_at) FROM {table}")
            return cur.fetchone()[0]
    finally:
        conn.close()


def read_validated(spark, query):
    """Batch-read the query result from Postgres via Spark JDBC."""
    url = (
        f"jdbc:postgresql://{os.getenv('PG_HOST')}:{os.getenv('PG_PORT', '5432')}"
        f"/{os.getenv('PG_DB', 'team42_db')}"
    )
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("driver", "org.postgresql.Driver")
        .option("query", query)
        .option("user", os.getenv("PG_USER"))
        .option("password", os.getenv("PG_PASSWORD"))
        .load()
    )


def load_once(spark):
    """Copy new validated rows into bronze once. Returns how many rows were read."""
    table = bronze_table()
    overlap = int(os.getenv("BRONZE_OVERLAP_SECONDS", "30"))

    since = read_resume_point(table)
    df = read_validated(spark, source_query(since, overlap))
    # persist: count and write reuse one read, and bronze_loaded_at is computed only once.
    df = df.withColumn("bronze_loaded_at", F.current_timestamp()).persist()
    try:
        rows = df.count()
        if rows:
            write_rows(df, table, BRONZE_COLUMNS, "ON CONFLICT (event_id) DO NOTHING")
        return rows
    finally:
        df.unpersist()


def build_spark():
    return (
        SparkSession.builder.appName("streamguard-bronze")
        .config("spark.jars.packages", POSTGRES_JDBC_PACKAGE)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def main():
    parser = argparse.ArgumentParser(description="Copy validated events into the bronze table.")
    parser.add_argument("--once", action="store_true", help="run one load and exit")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    poll_seconds = int(os.getenv("BRONZE_POLL_SECONDS", "5"))
    spark = build_spark()
    try:
        while True:
            try:
                rows = load_once(spark)
                log.info("read %d validated rows into %s", rows, bronze_table())
            except Exception as exc:  # keep the loop alive if Postgres is briefly down
                log.error("bronze load failed: %s", type(exc).__name__)
                if args.once:
                    raise
            if args.once:
                break
            time.sleep(poll_seconds)
    except KeyboardInterrupt:
        log.info("stopped")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
