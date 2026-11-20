"""Replay tests for src/processor/sinks.py.

Needs a Postgres with deploy/sql/init.sql applied and PG_URL/PG_USER/PG_PASSWORD set;
skipped automatically when PG_URL is not set. Each test uses fresh ids and deletes its rows.
"""

import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("PG_URL"),
    reason="PG_URL not set: needs Postgres with deploy/sql/init.sql applied",
)

from pyspark.sql import SparkSession  # noqa: E402

from src.processor.sinks import (  # noqa: E402
    add_alert_id,
    pg_connect,
    write_agg_1min,
    write_alerts,
    write_enriched_txn,
)

EVENT_TIME = datetime(2026, 11, 20, 10, 15, 30, tzinfo=timezone.utc)
WINDOW_START = datetime(2026, 11, 20, 10, 15, 0, tzinfo=timezone.utc)
CARD = "tok_3fa85f6457174562"

ALERT_SCHEMA = (
    "rule_id STRING, event_id STRING, card_token STRING, reason STRING, severity STRING, "
    "amount DECIMAL(12,2), event_time TIMESTAMP"
)
ENRICHED_SCHEMA = (
    "event_id STRING, card_token STRING, merchant_id STRING, merchant_category STRING, "
    "amount DECIMAL(12,2), currency STRING, country STRING, channel STRING, "
    "event_time TIMESTAMP, status STRING, card_avg_30d DECIMAL(12,2), is_blacklisted BOOLEAN"
)
AGG_SCHEMA = (
    "window_start TIMESTAMP, merchant_category STRING, country STRING, "
    "txn_count INT, total_amount DECIMAL(14,2), alert_count INT"
)


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)  # Spark workers use the venv Python
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test_sinks_idempotent")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def run_sql(sql, params=None):
    conn = pg_connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone() if cur.description else None
    finally:
        conn.close()


def new_ids(n):
    return [str(uuid.uuid4()) for _ in range(n)]


def test_writing_same_alerts_twice_leaves_count_unchanged(spark):
    event_ids = new_ids(4)
    rows = [
        ("velocity", eid, CARD, "6 txns in 1 minute", "HIGH", Decimal("249.99"), EVENT_TIME)
        for eid in event_ids
    ]
    df = add_alert_id(spark.createDataFrame(rows, ALERT_SCHEMA)).repartition(2)
    try:
        write_alerts(df)
        write_alerts(df)  # replay

        (count,) = run_sql(
            "SELECT count(*) FROM alerts WHERE event_id = ANY(%s::uuid[])", (event_ids,)
        )
        assert count == 4
    finally:
        run_sql("DELETE FROM alerts WHERE event_id = ANY(%s::uuid[])", (event_ids,))


def test_writing_same_enriched_batch_twice_leaves_count_unchanged(spark):
    event_ids = new_ids(4)
    rows = [
        (eid, CARD, "m_00421", "5411", Decimal("249.99"), "INR", "IN", "POS",
         EVENT_TIME, "APPROVED", Decimal("120.50"), False)
        for eid in event_ids
    ]
    df = spark.createDataFrame(rows, ENRICHED_SCHEMA).repartition(2)
    try:
        write_enriched_txn(df)
        write_enriched_txn(df)  # replay

        (count,) = run_sql(
            "SELECT count(*) FROM enriched_txn WHERE event_id = ANY(%s::uuid[])", (event_ids,)
        )
        assert count == 4
    finally:
        run_sql("DELETE FROM enriched_txn WHERE event_id = ANY(%s::uuid[])", (event_ids,))


def test_agg_1min_replay_replaces_not_adds(spark):
    category = f"test_{uuid.uuid4().hex[:8]}"  # unique key so real rows are never touched
    first = spark.createDataFrame(
        [(WINDOW_START, category, "IN", 10, Decimal("1000.00"), 1)], AGG_SCHEMA
    )
    updated = spark.createDataFrame(
        [(WINDOW_START, category, "IN", 12, Decimal("1200.00"), 2)], AGG_SCHEMA
    )
    lookup = (
        "SELECT count(*), max(txn_count), max(total_amount), max(alert_count) "
        "FROM agg_1min WHERE merchant_category = %s"
    )
    try:
        write_agg_1min(first)
        write_agg_1min(first)  # replay: must stay 10, not become 20
        assert run_sql(lookup, (category,)) == (1, 10, Decimal("1000.00"), 1)

        write_agg_1min(updated)  # newer running total replaces the old one
        assert run_sql(lookup, (category,)) == (1, 12, Decimal("1200.00"), 2)
    finally:
        run_sql("DELETE FROM agg_1min WHERE merchant_category = %s", (category,))
