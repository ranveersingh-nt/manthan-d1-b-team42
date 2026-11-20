"""Silver tests on small local Spark DataFrames (no database needed; skipped if Java is missing)."""

import os
import shutil
import sys
from datetime import datetime, timezone
from decimal import Decimal

import pytest

pytestmark = pytest.mark.skipif(
    not (shutil.which("java") or os.getenv("JAVA_HOME")), reason="Spark needs Java"
)

from pyspark.sql import SparkSession  # noqa: E402

from src.processor.silver import bronze_query, clean, enrich  # noqa: E402

NOW = datetime(2026, 11, 20, 10, 16, 10, tzinfo=timezone.utc)
CARD_1 = "tok_0000000000000001"  # baseline avg 137.01
CARD_NEW = "tok_00000000000fffff"  # no baseline yet
BAD_SHOP = "m_99001"  # on the blacklist
GOOD_SHOP = "m_00421"

PAYMENT_SCHEMA = "event_id STRING, card_token STRING, merchant_id STRING, amount DECIMAL(18,2), event_ts TIMESTAMP"


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)  # Spark workers use the venv Python
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_silver")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def lookups(spark):
    baseline = spark.createDataFrame(
        [(CARD_1, Decimal("137.01"))], "card_token STRING, avg_amount_30d DECIMAL(12,2)"
    )
    blacklist = spark.createDataFrame([(BAD_SHOP,)], "merchant_id STRING")
    return baseline, blacklist


def payments(spark, *rows):
    return spark.createDataFrame([(*r, NOW) for r in rows], PAYMENT_SCHEMA)


def by_event(df):
    return {r["event_id"]: r for r in df.collect()}


def test_first_run_reads_all_bronze_rows():
    sql = bronze_query(None, overlap_seconds=30)

    assert "FROM payment_events_bronze" in sql
    assert "WHERE" not in sql


def test_next_run_rereads_overlap_window():
    sql = bronze_query(NOW, overlap_seconds=30)

    assert "WHERE bronze_loaded_at >= '2026-11-20T10:15:40+00:00'::timestamptz" in sql


def test_clean_renames_event_ts_to_event_time(spark):
    cleaned = clean(payments(spark, ("e1", CARD_1, GOOD_SHOP, Decimal("10.00"))))

    assert "event_time" in cleaned.columns
    assert "event_ts" not in cleaned.columns


def test_clean_keeps_one_row_per_event_id(spark):
    cleaned = clean(
        payments(spark, ("e1", CARD_1, GOOD_SHOP, Decimal("10.00")), ("e1", CARD_1, GOOD_SHOP, Decimal("10.00")))
    )

    assert cleaned.count() == 1


def test_clean_turns_oversized_amount_into_null(spark):
    cleaned = clean(payments(spark, ("e1", CARD_1, GOOD_SHOP, Decimal("99999999999.00"))))

    assert cleaned.first()["amount"] is None


def test_enrich_card_with_baseline_gets_its_average(spark, lookups):
    rows = by_event(enrich(clean(payments(spark, ("e1", CARD_1, GOOD_SHOP, Decimal("900.00")))), *lookups))

    assert rows["e1"]["card_avg_30d"] == Decimal("137.01")


def test_enrich_card_without_baseline_gets_null_average(spark, lookups):
    rows = by_event(enrich(clean(payments(spark, ("e1", CARD_NEW, GOOD_SHOP, Decimal("50.00")))), *lookups))

    assert rows["e1"]["card_avg_30d"] is None


def test_enrich_blacklisted_merchant_is_flagged(spark, lookups):
    rows = by_event(enrich(clean(payments(spark, ("e1", CARD_1, BAD_SHOP, Decimal("100.00")))), *lookups))

    assert rows["e1"]["is_blacklisted"] is True


def test_enrich_normal_merchant_is_not_flagged(spark, lookups):
    rows = by_event(enrich(clean(payments(spark, ("e1", CARD_1, GOOD_SHOP, Decimal("100.00")))), *lookups))

    assert rows["e1"]["is_blacklisted"] is False


def test_enrich_never_drops_a_payment(spark, lookups):
    df = payments(
        spark,
        ("e1", CARD_1, BAD_SHOP, Decimal("900.00")),  # both lookups match
        ("e2", CARD_NEW, GOOD_SHOP, Decimal("50.00")),  # neither matches
    )

    assert enrich(clean(df), *lookups).count() == 2
