"""Silver tests on small local Spark DataFrames (no database needed; skipped if Java is missing)."""

import os
import shutil
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

pytestmark = pytest.mark.skipif(
    not (shutil.which("java") or os.getenv("JAVA_HOME")), reason="Spark needs Java"
)

from pyspark.sql import SparkSession  # noqa: E402

from src.processor.silver import (  # noqa: E402
    add_reject_reason,
    bronze_query,
    clean,
    enrich,
    load_silver_checks,
)

NOW = datetime(2026, 11, 20, 10, 16, 10, tzinfo=timezone.utc)
CARD_1 = "tok_0000000000000001"  # baseline avg 137.01
CARD_NEW = "tok_00000000000fffff"  # no baseline yet
BAD_SHOP = "m_99001"  # on the blacklist
GOOD_SHOP = "m_00421"
CHECKS = load_silver_checks("src/processor/config/silver_checks.yml")

PAYMENT_SCHEMA = (
    "event_id STRING, card_token STRING, merchant_id STRING, merchant_category STRING, "
    "amount DECIMAL(18,2), currency STRING, country STRING, channel STRING, "
    "event_ts TIMESTAMP, status STRING"
)


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


def pay(event_id="e1", card=CARD_1, merchant=GOOD_SHOP, amount="100.00", currency="INR",
        country="IN", channel="pos", status="approved", event_ts=NOW, category="5411"):
    """One bronze payment; defaults are valid, and use the contract's lowercase channel/status."""
    return (event_id, card, merchant, category, Decimal(amount), currency, country, channel, event_ts, status)


def payments(spark, *rows):
    return spark.createDataFrame(list(rows), PAYMENT_SCHEMA)


def reject_reason(spark, row):
    return add_reject_reason(clean(payments(spark, row)), CHECKS).first()["reject_reason"]


def by_event(df):
    return {r["event_id"]: r for r in df.collect()}


# --- bronze query

def test_first_run_reads_all_bronze_rows():
    sql = bronze_query(None, overlap_seconds=30)

    assert "FROM payment_events_bronze" in sql
    assert "WHERE" not in sql


def test_next_run_rereads_overlap_window():
    sql = bronze_query(NOW, overlap_seconds=30)

    assert "WHERE bronze_loaded_at >= '2026-11-20T10:15:40+00:00'::timestamptz" in sql


# --- clean

def test_clean_renames_event_ts_to_event_time(spark):
    cleaned = clean(payments(spark, pay()))

    assert "event_time" in cleaned.columns
    assert "event_ts" not in cleaned.columns


def test_clean_uppercases_lowercase_channel_status_currency_country(spark):
    row = clean(payments(spark, pay(channel=" pos ", status="approved", currency="inr", country="in"))).first()

    assert (row["channel"], row["status"], row["currency"], row["country"]) == ("POS", "APPROVED", "INR", "IN")


def test_clean_keeps_one_row_per_event_id(spark):
    assert clean(payments(spark, pay("e1"), pay("e1"))).count() == 1


def test_clean_turns_oversized_amount_into_null(spark):
    assert clean(payments(spark, pay(amount="99999999999.00"))).first()["amount"] is None


# --- business checks: one passing and one failing case each

def test_valid_payment_has_no_reject_reason(spark):
    assert reject_reason(spark, pay()) is None


def test_reversed_moto_payment_is_accepted(spark):
    assert reject_reason(spark, pay(channel="moto", status="reversed")) is None


def test_zero_amount_is_rejected(spark):
    assert reject_reason(spark, pay(amount="0.00")) == "amount must be greater than 0"


def test_negative_amount_is_rejected(spark):
    assert reject_reason(spark, pay(amount="-5.00")) == "amount must be greater than 0"


def test_amount_above_ceiling_is_rejected(spark):
    assert reject_reason(spark, pay(amount="1000000.01")).startswith("amount is above the ceiling")


def test_amount_equal_to_ceiling_is_accepted(spark):
    assert reject_reason(spark, pay(amount="1000000.00")) is None


def test_oversized_amount_is_rejected(spark):
    assert reject_reason(spark, pay(amount="99999999999.00")) == "amount does not fit NUMERIC(12,2)"


def test_currency_not_in_list_is_rejected(spark):
    assert reject_reason(spark, pay(currency="USDT")) == "currency is not in the allowed list"


def test_three_letter_country_is_rejected(spark):
    assert reject_reason(spark, pay(country="IND")) == "country must be 2 letters"


def test_unknown_channel_is_rejected(spark):
    assert reject_reason(spark, pay(channel="kiosk")) == "channel is not allowed"


def test_unknown_status_is_rejected(spark):
    assert reject_reason(spark, pay(status="refunded")) == "status is not allowed"


def test_event_more_than_24h_in_future_is_rejected(spark):
    future = datetime.now(timezone.utc) + timedelta(hours=25)

    assert reject_reason(spark, pay(event_ts=future)) == "event time is more than 24h in the future"


def test_event_in_the_past_is_accepted(spark):
    assert reject_reason(spark, pay(event_ts=NOW - timedelta(days=1))) is None


def test_missing_merchant_category_is_rejected(spark):
    assert reject_reason(spark, pay(category=None)) == "required field is missing"


# --- config

def test_silver_checks_yml_loads(tmp_path):
    assert CHECKS["amount_ceiling"] == Decimal("1000000.00")
    assert "MOTO" in CHECKS["allowed_channels"]


def test_negative_ceiling_is_rejected(tmp_path):
    path = tmp_path / "checks.yml"
    path.write_text(
        "amount_ceiling: -1\nallowed_currencies: [INR]\nallowed_channels: [POS]\n"
        "allowed_statuses: [APPROVED]\nmax_future_hours: 24\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="amount_ceiling"):
        load_silver_checks(path)


# --- enrich

def test_enrich_card_with_baseline_gets_its_average(spark, lookups):
    rows = by_event(enrich(clean(payments(spark, pay(amount="900.00"))), *lookups))

    assert rows["e1"]["card_avg_30d"] == Decimal("137.01")


def test_enrich_card_without_baseline_gets_null_average(spark, lookups):
    rows = by_event(enrich(clean(payments(spark, pay(card=CARD_NEW))), *lookups))

    assert rows["e1"]["card_avg_30d"] is None


def test_enrich_blacklisted_merchant_is_flagged(spark, lookups):
    rows = by_event(enrich(clean(payments(spark, pay(merchant=BAD_SHOP))), *lookups))

    assert rows["e1"]["is_blacklisted"] is True


def test_enrich_normal_merchant_is_not_flagged(spark, lookups):
    rows = by_event(enrich(clean(payments(spark, pay())), *lookups))

    assert rows["e1"]["is_blacklisted"] is False


def test_enrich_never_drops_a_payment(spark, lookups):
    df = payments(
        spark,
        pay("e1", merchant=BAD_SHOP, amount="900.00"),  # both lookups match
        pay("e2", card=CARD_NEW),  # neither matches
    )

    assert enrich(clean(df), *lookups).count() == 2
