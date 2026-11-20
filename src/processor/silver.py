"""Silver: take new payments from bronze, add the card and merchant lookups, save to enriched_txn.

Every SILVER_POLL_SECONDS:
  1. read silver's mark: the last bronze_loaded_at it already took          (marks.py)
  2. read newer payments from payment_events_bronze                         (small overlap)
  3. clean: event_ts -> event_time, amount -> DECIMAL(12,2), one row per event_id
  4. enrich, using the payment's two columns:
       card_token  -> card_baseline      -> card_avg_30d   (NULL if the card has no baseline)
       merchant_id -> merchant_blacklist -> is_blacklisted (true / false)
  5. write enriched_txn (ON CONFLICT DO NOTHING), and only then save the new mark

If silver stops halfway, the mark is not saved, so the next run re-reads those rows;
the idempotent write makes that safe.

Run:  python -m src.processor.silver            (loop forever, Ctrl+C to stop)
      python -m src.processor.silver --once     (one run)
"""

import argparse
import logging
import os
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv
from pyspark.sql import functions as F

from src.processor.bronze import BRONZE_COLUMNS, bronze_table, build_spark
from src.processor.marks import read_mark, save_mark
from src.processor.sinks import ISO_TS_FORMAT, read_jdbc, write_enriched_txn

log = logging.getLogger("silver")

MARK_NAME = "silver_bronze_loaded_at"


def bronze_query(since, overlap_seconds, table="payment_events_bronze"):
    """SQL for the bronze rows to take. since=None (first run) means take everything."""
    sql = f"SELECT {', '.join(BRONZE_COLUMNS)} FROM {table}"
    if since is None:
        return sql
    start = since - timedelta(seconds=overlap_seconds)
    return f"{sql} WHERE bronze_loaded_at >= '{start.isoformat()}'::timestamptz"


def clean(bronze_df):
    """Match the enriched_txn shape: rename the time column, fit amount, one row per event."""
    return (
        bronze_df.withColumnRenamed("event_ts", "event_time")
        # bronze allows NUMERIC(18,2), enriched_txn NUMERIC(12,2): try_cast gives NULL
        # (instead of failing the whole batch, ANSI mode) for amounts that don't fit.
        .withColumn("amount", F.expr("try_cast(amount AS DECIMAL(12,2))"))
        .dropDuplicates(["event_id"])
    )


def enrich(payments_df, baseline_df, blacklist_df):
    """Add card_avg_30d and is_blacklisted. The payment row is what links the two lookups.

    Left joins, so a payment is never dropped: no baseline -> card_avg_30d NULL,
    merchant not on the list -> is_blacklisted false. Both lookup tables are small,
    so they are broadcast (copied to every executor, like a read-only HashMap).
    """
    baseline = baseline_df.select("card_token", F.col("avg_amount_30d").alias("card_avg_30d"))
    blacklist = blacklist_df.select("merchant_id").distinct().withColumn("is_blacklisted", F.lit(True))
    return (
        payments_df.join(F.broadcast(baseline), on="card_token", how="left")
        .join(F.broadcast(blacklist), on="merchant_id", how="left")
        .withColumn("is_blacklisted", F.coalesce(F.col("is_blacklisted"), F.lit(False)))
    )


def load_once(spark):
    """Move new bronze payments into enriched_txn once. Returns (rows read, rows skipped)."""
    overlap = int(os.getenv("SILVER_OVERLAP_SECONDS", "30"))

    since = read_mark(MARK_NAME)
    bronze_df = read_jdbc(spark, bronze_query(since, overlap, bronze_table())).persist()
    try:
        rows = bronze_df.count()
        if rows == 0:
            return 0, 0

        # New mark as an ISO string with offset, so it never gets a local-timezone shift.
        new_mark = bronze_df.agg(F.date_format(F.max("bronze_loaded_at"), ISO_TS_FORMAT)).first()[0]

        payments = clean(bronze_df)
        skipped = payments.filter(F.col("amount").isNull()).count()
        if skipped:
            log.warning("skipped %d payments whose amount does not fit NUMERIC(12,2)", skipped)

        enriched = enrich(
            payments.filter(F.col("amount").isNotNull()),
            read_jdbc(spark, "SELECT card_token, avg_amount_30d FROM card_baseline"),
            read_jdbc(spark, "SELECT merchant_id FROM merchant_blacklist"),
        )
        write_enriched_txn(enriched)
        save_mark(MARK_NAME, datetime.fromisoformat(new_mark))  # only after a successful write
        return rows, skipped
    finally:
        bronze_df.unpersist()


def main():
    parser = argparse.ArgumentParser(description="Enrich bronze payments into enriched_txn.")
    parser.add_argument("--once", action="store_true", help="run one load and exit")
    args = parser.parse_args()

    # Load .env BEFORE starting Spark, so the Spark worker processes inherit PG_URL etc.
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    poll_seconds = int(os.getenv("SILVER_POLL_SECONDS", "5"))
    spark = build_spark("streamguard-silver")
    try:
        while True:
            try:
                rows, skipped = load_once(spark)
                log.info("read %d bronze rows into enriched_txn (%d skipped)", rows, skipped)
            except Exception as exc:  # keep the loop alive if Postgres is briefly down
                log.error("silver load failed: %s", type(exc).__name__)
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
