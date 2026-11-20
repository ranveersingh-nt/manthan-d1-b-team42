"""Idempotent Postgres writers for Spark DataFrames, one micro-batch at a time.

Call them from foreachBatch, for example:
    alerts_df.writeStream.foreachBatch(lambda batch_df, batch_id: write_alerts(batch_df)).start()

Idempotent = writing the same batch twice gives the same table contents, so Spark task
retries and Kafka replays never create duplicates or double counts.

ON CONFLICT in Java terms: it is like map.putIfAbsent(key, row) (DO NOTHING) or
map.put(key, row) (DO UPDATE), done by the database in one atomic statement.
"""

import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

PAGE_SIZE = 1000

# ISO 8601 with an explicit offset (e.g. 2026-11-20T10:15:30.000000Z). PySpark otherwise hands
# Python naive local-time datetimes, which Postgres could read in the wrong timezone.
ISO_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"

# Column lists match deploy/sql/init.sql. processed_at / created_at use the DB default now().
ENRICHED_TXN_COLUMNS = [
    "event_id", "card_token", "merchant_id", "merchant_category", "amount", "currency",
    "country", "channel", "event_time", "status", "card_avg_30d", "is_blacklisted",
]
AGG_1MIN_COLUMNS = [
    "window_start", "merchant_category", "country", "txn_count", "total_amount", "alert_count",
]
ALERTS_COLUMNS = [
    "alert_id", "rule_id", "event_id", "card_token", "reason", "severity", "amount", "event_time",
]


def add_alert_id(df):
    """Add alert_id = sha256(rule_id + '|' + event_id): same rule + same event = same id."""
    key = F.concat(F.col("rule_id"), F.lit("|"), F.col("event_id").cast("string"))
    return df.withColumn("alert_id", F.sha2(key, 256))


def write_enriched_txn(df):
    """Insert events; an event_id that is already there is skipped (putIfAbsent)."""
    write_rows(df, "enriched_txn", ENRICHED_TXN_COLUMNS, "ON CONFLICT (event_id) DO NOTHING")


def write_agg_1min(df):
    """Upsert per-minute totals. Values are REPLACED, never added, so a replay can't double them.

    Expects one row per (window_start, merchant_category, country) per batch, which a Spark
    groupBy gives; Postgres rejects the same key twice in one DO UPDATE statement.
    """
    write_rows(
        df,
        "agg_1min",
        AGG_1MIN_COLUMNS,
        "ON CONFLICT (window_start, merchant_category, country) DO UPDATE SET "
        "txn_count = EXCLUDED.txn_count, "
        "total_amount = EXCLUDED.total_amount, "
        "alert_count = EXCLUDED.alert_count",
    )


def write_alerts(df):
    """Insert alerts; an alert_id that is already there is skipped (putIfAbsent)."""
    write_rows(df, "alerts", ALERTS_COLUMNS, "ON CONFLICT (alert_id) DO NOTHING")


def _pg_url():
    """PG_URL from .env, e.g. jdbc:postgresql://localhost:5432/team42_db (same as src/dq)."""
    url = os.getenv("PG_URL")
    if not url:
        raise ValueError("PG_URL is not set (add it to your local .env)")
    return url


def jdbc_url():
    """PG_URL in JDBC form, for Spark's JDBC reader."""
    url = _pg_url()
    return url if url.startswith("jdbc:") else f"jdbc:{url}"


def read_jdbc(spark, query):
    """Batch-read a SQL query's result from Postgres into a Spark DataFrame (Spark JDBC)."""
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url())
        .option("driver", "org.postgresql.Driver")
        .option("query", query)
        .option("user", os.getenv("PG_USER"))
        .option("password", os.getenv("PG_PASSWORD"))
        .load()
    )


def pg_connect():
    """psycopg2 connection from PG_URL (JDBC or libpq URI), PG_USER, PG_PASSWORD.

    Never log these values; the password stays out of all output.
    """
    url = _pg_url()
    if url.startswith("jdbc:"):
        url = url[len("jdbc:"):]  # psycopg2 wants postgresql://host:port/db
    return psycopg2.connect(url, user=os.getenv("PG_USER"), password=os.getenv("PG_PASSWORD"))


def write_rows(df, table, columns, on_conflict_sql):
    """Write `columns` of df into `table`, one transaction per partition. Shared by all writers."""
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s {on_conflict_sql}"

    # Select by name, so the caller's column order doesn't matter; timestamps become ISO strings.
    ts_columns = {f.name for f in df.schema.fields if isinstance(f.dataType, TimestampType)}
    rows_df = df.select(
        *[
            F.date_format(F.col(c), ISO_TS_FORMAT).alias(c) if c in ts_columns else F.col(c)
            for c in columns
        ]
    )

    # foreachPartition in Java terms: like submitting one Runnable per chunk of rows to an
    # ExecutorService. Spark serializes this function and runs it on the executors, so each
    # partition opens its own connection there (a DB connection can't be shipped between JVMs).
    def write_partition(rows):
        batch = [tuple(row) for row in rows]
        if not batch:
            return  # empty partition: don't open a connection
        conn = pg_connect()
        try:
            # `with conn` = one transaction: commit on success, rollback on any exception
            # (like try { ...; commit(); } catch (e) { rollback(); throw e; }).
            # If the task fails, Spark retries it; the upserts make that retry safe.
            with conn:
                with conn.cursor() as cur:
                    execute_values(cur, sql, batch, page_size=PAGE_SIZE)
        finally:
            conn.close()

    rows_df.foreachPartition(write_partition)
