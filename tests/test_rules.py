"""Fraud rule tests: one flagged and one not-flagged case per rule, on local Spark (no database)."""

import copy
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

from src.processor.rules import lookback_minutes, run_rules  # noqa: E402
from src.processor.rules_config import load_rules  # noqa: E402

RULES = load_rules("src/processor/config/rules.yml")  # real thresholds: 5/1min, 2/30min, 5x
T0 = datetime(2026, 11, 20, 10, 0, 0, tzinfo=timezone.utc)
CARD = "tok_0000000000000001"
SCHEMA = (
    "event_id STRING, card_token STRING, merchant_id STRING, amount DECIMAL(12,2), "
    "country STRING, event_time TIMESTAMP, card_avg_30d DECIMAL(12,2), "
    "is_blacklisted BOOLEAN, is_new BOOLEAN"
)


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)  # Spark workers use the venv Python
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_rules")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def pay(event_id, seconds=0, amount="100.00", country="IN", merchant="m_00421",
        avg="137.01", blacklisted=False, new=True, card=CARD):
    return (
        event_id, card, merchant, Decimal(amount), country, T0 + timedelta(seconds=seconds),
        Decimal(avg) if avg is not None else None, blacklisted, new,
    )


def alerted(spark, rows, rule, rules_cfg=RULES):
    """Event ids that raised an alert for `rule`."""
    alerts = run_rules(spark.createDataFrame(rows, SCHEMA), rules_cfg)
    return {r["event_id"] for r in alerts.filter(alerts.rule_id == rule).collect()}


# --- velocity: more than 5 payments per card within 1 minute

def test_velocity_flags_sixth_payment_within_one_minute(spark):
    rows = [pay(f"e{i}", seconds=10 * i) for i in range(1, 7)]  # 6 payments in 50 s

    assert alerted(spark, rows, "velocity") == {"e6"}


def test_velocity_does_not_flag_five_payments_within_one_minute(spark):
    rows = [pay(f"e{i}", seconds=10 * i) for i in range(1, 6)]

    assert alerted(spark, rows, "velocity") == set()


def test_velocity_does_not_count_payments_older_than_one_minute(spark):
    rows = [pay(f"e{i}", seconds=15 * i) for i in range(1, 7)]  # 6 payments over 75 s

    assert alerted(spark, rows, "velocity") == set()


# --- impossible travel: 2 countries per card within 30 minutes

def test_impossible_travel_flags_two_countries_within_30_minutes(spark):
    rows = [pay("e1", seconds=0, country="IN"), pay("e2", seconds=10 * 60, country="US")]

    assert alerted(spark, rows, "impossible_travel") == {"e2"}


def test_impossible_travel_does_not_flag_two_countries_31_minutes_apart(spark):
    rows = [pay("e1", seconds=0, country="IN"), pay("e2", seconds=31 * 60, country="US")]

    assert alerted(spark, rows, "impossible_travel") == set()


# --- amount spike: amount > 5 x card's 30-day average (137.01 -> limit 685.05)

def test_amount_spike_flags_amount_above_five_times_average(spark):
    assert alerted(spark, [pay("e1", amount="685.06")], "amount_spike") == {"e1"}


def test_amount_spike_does_not_flag_exactly_five_times_average(spark):
    assert alerted(spark, [pay("e1", amount="685.05")], "amount_spike") == set()


def test_amount_spike_skips_card_without_baseline(spark):
    assert alerted(spark, [pay("e1", amount="99999.00", avg=None)], "amount_spike") == set()


# --- merchant blacklist

def test_merchant_blacklist_flags_blacklisted_merchant(spark):
    rows = [pay("e1", merchant="m_99001", blacklisted=True)]

    assert alerted(spark, rows, "merchant_blacklist") == {"e1"}


def test_merchant_blacklist_does_not_flag_normal_merchant(spark):
    assert alerted(spark, [pay("e1")], "merchant_blacklist") == set()


# --- behaviour shared by all rules

def test_history_rows_count_but_never_raise_alerts(spark):
    # 5 old payments (already judged) + 1 new one within the minute -> only the new one is alerted
    rows = [pay(f"old{i}", seconds=5 * i, new=False, blacklisted=True) for i in range(1, 6)]
    rows.append(pay("e6", seconds=30))

    assert alerted(spark, rows, "velocity") == {"e6"}
    assert alerted(spark, rows, "merchant_blacklist") == set()


def test_disabled_rule_raises_no_alerts(spark):
    cfg = copy.deepcopy(RULES)
    cfg["merchant_blacklist"]["enabled"] = False

    assert alerted(spark, [pay("e1", blacklisted=True)], "merchant_blacklist", cfg) == set()


def test_alert_id_is_the_same_on_replay(spark):
    rows = [pay("e1", merchant="m_99001", blacklisted=True)]
    first = run_rules(spark.createDataFrame(rows, SCHEMA), RULES).first()["alert_id"]
    second = run_rules(spark.createDataFrame(rows, SCHEMA), RULES).first()["alert_id"]

    assert first == second and len(first) == 64


def test_lookback_is_longest_enabled_window():
    cfg = copy.deepcopy(RULES)
    assert lookback_minutes(cfg) == 30

    cfg["impossible_travel"]["enabled"] = False
    assert lookback_minutes(cfg) == 1
