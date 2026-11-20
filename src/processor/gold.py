"""Gold: run the fraud rules on new enriched payments and update the dashboard totals.

Every GOLD_POLL_SECONDS:
  1. read gold's mark: the last enriched_txn.processed_at it already handled    (marks.py)
  2. read the new enriched_txn rows (small overlap)
  3. read the history the time-window rules need: enriched_txn rows from
     (earliest new event_time - longest rule window) up to the latest new event_time
  4. run the enabled rules from rules.yml on history + new rows; only new rows raise alerts
  5. write alerts                (ON CONFLICT (alert_id) DO NOTHING: a replay adds nothing)
  6. recount every minute the new rows touched, from enriched_txn + alerts, and write
     agg_1min (DO UPDATE: full totals replace old ones, so counts never double)
  7. only then save the new mark

Run:  python -m src.processor.gold            (loop forever, Ctrl+C to stop)
      python -m src.processor.gold --once     (one run)
"""

import argparse
import logging
import os
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv
from pyspark.sql import functions as F

from src.processor.bronze import build_spark
from src.processor.marks import read_mark, save_mark
from src.processor.rules import lookback_minutes, run_rules
from src.processor.rules_config import load_rules
from src.processor.sinks import ISO_TS_FORMAT, read_jdbc, write_agg_1min, write_alerts

log = logging.getLogger("gold")

MARK_NAME = "gold_processed_at"
CONTEXT_COLUMNS = [
    "event_id", "card_token", "merchant_id", "merchant_category", "amount", "country",
    "event_time", "card_avg_30d", "is_blacklisted",
]


def new_rows_query(since, overlap_seconds):
    """New enriched rows to judge. since=None (first run) means all rows."""
    sql = "SELECT event_id, event_time, processed_at FROM enriched_txn"
    if since is None:
        return sql
    start = since - timedelta(seconds=overlap_seconds)
    return f"{sql} WHERE processed_at >= '{start.isoformat()}'::timestamptz"


def context_query(min_event_time, max_event_time, lookback):
    """History + new rows: everything the time-window rules need to see."""
    return (
        f"SELECT {', '.join(CONTEXT_COLUMNS)} FROM enriched_txn "
        f"WHERE event_time >= '{min_event_time}'::timestamptz - interval '{lookback} minutes' "
        f"AND event_time <= '{max_event_time}'::timestamptz"
    )


def agg_query(min_event_time, max_event_time):
    """Full per-minute totals for every minute from the earliest to the latest new event.

    Recounting whole minutes (not just adding the new rows) is what makes the
    replace-don't-add upsert in write_agg_1min correct.
    """
    lo = f"date_trunc('minute', '{min_event_time}'::timestamptz)"
    hi = f"date_trunc('minute', '{max_event_time}'::timestamptz) + interval '1 minute'"
    return (
        "SELECT date_trunc('minute', e.event_time) AS window_start, e.merchant_category, e.country, "
        "count(*)::int AS txn_count, sum(e.amount)::numeric(14,2) AS total_amount, "
        "coalesce(sum(a.n), 0)::int AS alert_count "
        "FROM enriched_txn e "
        "LEFT JOIN (SELECT event_id, count(*) AS n FROM alerts "
        f"WHERE event_time >= {lo} AND event_time < {hi} GROUP BY event_id) a "
        "ON a.event_id = e.event_id "
        f"WHERE e.event_time >= {lo} AND e.event_time < {hi} "
        "GROUP BY 1, 2, 3"
    )


def load_once(spark, rules_cfg):
    """Judge new enriched rows once. Returns (new rows read, alerts raised)."""
    overlap = int(os.getenv("GOLD_OVERLAP_SECONDS", "30"))

    since = read_mark(MARK_NAME)
    new_df = read_jdbc(spark, new_rows_query(since, overlap)).persist()
    try:
        rows = new_df.count()
        if rows == 0:
            return 0, 0

        # Bounds as ISO strings with offset, so no local-timezone shift sneaks in.
        min_et, max_et, new_mark = new_df.agg(
            F.date_format(F.min("event_time"), ISO_TS_FORMAT),
            F.date_format(F.max("event_time"), ISO_TS_FORMAT),
            F.date_format(F.max("processed_at"), ISO_TS_FORMAT),
        ).first()

        context = read_jdbc(spark, context_query(min_et, max_et, lookback_minutes(rules_cfg)))
        new_ids = new_df.select("event_id").withColumn("is_new", F.lit(True))
        events = context.join(F.broadcast(new_ids), on="event_id", how="left").fillna({"is_new": False})

        raised = 0
        alerts = run_rules(events, rules_cfg)
        if alerts is not None:
            alerts = alerts.persist()
            try:
                raised = alerts.count()
                if raised:
                    write_alerts(alerts)
            finally:
                alerts.unpersist()

        # After the alerts are stored, so alert_count includes them.
        write_agg_1min(read_jdbc(spark, agg_query(min_et, max_et)))
        save_mark(MARK_NAME, datetime.fromisoformat(new_mark))  # only after both writes succeed
        return rows, raised
    finally:
        new_df.unpersist()


def main():
    parser = argparse.ArgumentParser(description="Run fraud rules on enriched_txn into alerts and agg_1min.")
    parser.add_argument("--once", action="store_true", help="run one load and exit")
    args = parser.parse_args()

    # Load .env BEFORE starting Spark, so the Spark worker processes inherit PG_URL etc.
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    rules_cfg = load_rules()  # fails fast with a clear message if rules.yml is invalid
    poll_seconds = int(os.getenv("GOLD_POLL_SECONDS", "5"))
    spark = build_spark("streamguard-gold")
    try:
        while True:
            try:
                rows, raised = load_once(spark, rules_cfg)
                log.info("judged %d enriched rows, raised %d alerts (replays are skipped on write)", rows, raised)
            except Exception as exc:  # keep the loop alive if Postgres is briefly down
                log.error("gold load failed: %s", type(exc).__name__)
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
