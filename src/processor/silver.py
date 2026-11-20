"""Silver: take new payments from bronze, check them, add the card and merchant lookups, save to enriched_txn.

Every SILVER_POLL_SECONDS:
  1. read silver's mark: the last bronze_loaded_at it already took          (marks.py)
  2. read newer payments from payment_events_bronze                         (small overlap)
  3. clean: event_ts -> event_time, amount -> DECIMAL(12,2), upper-case channel / status /
     currency / country, one row per event_id
  4. business checks from config/silver_checks.yml (amount range, currency list, country,
     channel, status, event time <= +24h). Failing payments are skipped and logged with the
     reason, so one bad row can never block silver.
  5. enrich, using the payment's two columns:
       card_token  -> card_baseline      -> card_avg_30d   (NULL if the card has no baseline)
       merchant_id -> merchant_blacklist -> is_blacklisted (true / false)
  6. write enriched_txn (ON CONFLICT DO NOTHING), and only then save the new mark

If silver stops halfway, the mark is not saved, so the next run re-reads those rows;
the idempotent write makes that safe.

Run:  python -m src.processor.silver            (loop forever, Ctrl+C to stop)
      python -m src.processor.silver --once     (one run)
"""

import argparse
import logging
import operator
import os
import time
from datetime import datetime, timedelta
from decimal import Decimal
from functools import reduce

import yaml
from dotenv import load_dotenv
from pyspark.sql import functions as F

from src.processor.bronze import BRONZE_COLUMNS, bronze_table, build_spark
from src.processor.marks import read_mark, save_mark
from src.processor.sinks import ISO_TS_FORMAT, read_jdbc, write_enriched_txn

log = logging.getLogger("silver")

MARK_NAME = "silver_bronze_loaded_at"
UPPERCASE_COLUMNS = ("channel", "status", "currency", "country")
REQUIRED_COLUMNS = ("event_id", "card_token", "merchant_id", "merchant_category", "event_time")
CHECK_LISTS = ("allowed_currencies", "allowed_channels", "allowed_statuses")
MAX_AMOUNT_NUMERIC_12_2 = Decimal("9999999999.99")


def load_silver_checks(path=None):
    """Read and validate config/silver_checks.yml. Raises ValueError with a clear message."""
    path = path or os.getenv("SILVER_CHECKS_PATH", "src/processor/config/silver_checks.yml")
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    ceiling = cfg.get("amount_ceiling")
    if isinstance(ceiling, bool) or not isinstance(ceiling, (int, float)) or not 0 < ceiling <= MAX_AMOUNT_NUMERIC_12_2:
        raise ValueError(f"{path}: amount_ceiling must be a number > 0 that fits NUMERIC(12,2), got {ceiling!r}")
    hours = cfg.get("max_future_hours")
    if isinstance(hours, bool) or not isinstance(hours, int) or hours <= 0:
        raise ValueError(f"{path}: max_future_hours must be a whole number > 0, got {hours!r}")
    for key in CHECK_LISTS:
        values = cfg.get(key)
        if not isinstance(values, list) or not values or not all(isinstance(v, str) for v in values):
            raise ValueError(f"{path}: {key} must be a non-empty list of strings")
        cfg[key] = [v.strip().upper() for v in values]

    cfg["amount_ceiling"] = Decimal(str(ceiling))
    return cfg


def bronze_query(since, overlap_seconds, table="payment_events_bronze"):
    """SQL for the bronze rows to take. since=None (first run) means take everything."""
    sql = f"SELECT {', '.join(BRONZE_COLUMNS)} FROM {table}"
    if since is None:
        return sql
    start = since - timedelta(seconds=overlap_seconds)
    return f"{sql} WHERE bronze_loaded_at >= '{start.isoformat()}'::timestamptz"


def clean(bronze_df):
    """Match the enriched_txn shape: rename the time column, fit amount, normalise case, one row per event."""
    df = (
        bronze_df.withColumnRenamed("event_ts", "event_time")
        # bronze allows NUMERIC(18,2), enriched_txn NUMERIC(12,2): try_cast gives NULL
        # (instead of failing the whole batch, ANSI mode) for amounts that don't fit.
        .withColumn("amount", F.expr("try_cast(amount AS DECIMAL(12,2))"))
        .withColumn("merchant_category", F.trim("merchant_category"))
    )
    for column in UPPERCASE_COLUMNS:  # the contract allows 'pos' / 'approved'; the tables use 'POS' / 'APPROVED'
        df = df.withColumn(column, F.upper(F.trim(column)))
    return df.dropDuplicates(["event_id"])


def _fails(condition):
    """A NULL check result counts as a failure, so a missing value can't slip through."""
    return F.coalesce(condition, F.lit(True))


def add_reject_reason(df, checks):
    """Add reject_reason: the first business check the payment fails, or NULL if it passes all."""
    hours = checks["max_future_hours"]
    future_limit = F.expr(f"current_timestamp() + INTERVAL {hours} HOURS")
    ceiling = checks["amount_ceiling"]
    rules = [
        (reduce(operator.or_, [F.col(c).isNull() for c in REQUIRED_COLUMNS]), "required field is missing"),
        (F.col("amount").isNull(), "amount does not fit NUMERIC(12,2)"),
        (_fails(F.col("amount") <= 0), "amount must be greater than 0"),
        (_fails(F.col("amount") > F.lit(ceiling)), f"amount is above the ceiling {ceiling}"),
        (_fails(~F.col("currency").isin(*checks["allowed_currencies"])), "currency is not in the allowed list"),
        (_fails(~F.col("country").rlike("^[A-Z]{2}$")), "country must be 2 letters"),
        (_fails(~F.col("channel").isin(*checks["allowed_channels"])), "channel is not allowed"),
        (_fails(~F.col("status").isin(*checks["allowed_statuses"])), "status is not allowed"),
        (_fails(F.col("event_time") > future_limit), f"event time is more than {hours}h in the future"),
    ]
    # coalesce picks the first non-NULL: like a chain of if / else-if returning the first failure.
    return df.withColumn("reject_reason", F.coalesce(*[F.when(cond, F.lit(msg)) for cond, msg in rules]))


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


def load_once(spark, checks):
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

        payments = add_reject_reason(clean(bronze_df), checks)

        # Log how many payments failed each check (reasons only, never field values).
        skipped = 0
        for r in payments.filter(F.col("reject_reason").isNotNull()).groupBy("reject_reason").count().collect():
            skipped += r["count"]
            log.warning("skipped %d payments: %s", r["count"], r["reject_reason"])

        enriched = enrich(
            payments.filter(F.col("reject_reason").isNull()).drop("reject_reason"),
            read_jdbc(spark, "SELECT card_token, avg_amount_30d FROM card_baseline"),
            read_jdbc(spark, "SELECT merchant_id FROM merchant_blacklist"),
        )
        write_enriched_txn(enriched)
        save_mark(MARK_NAME, datetime.fromisoformat(new_mark))  # only after a successful write
        return rows, skipped
    finally:
        bronze_df.unpersist()


def main():
    parser = argparse.ArgumentParser(description="Check and enrich bronze payments into enriched_txn.")
    parser.add_argument("--once", action="store_true", help="run one load and exit")
    args = parser.parse_args()

    # Load .env BEFORE starting Spark, so the Spark worker processes inherit PG_URL etc.
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    checks = load_silver_checks()  # fails fast with a clear message if the config is invalid
    poll_seconds = int(os.getenv("SILVER_POLL_SECONDS", "5"))
    spark = build_spark("streamguard-silver")
    try:
        while True:
            try:
                rows, skipped = load_once(spark, checks)
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
