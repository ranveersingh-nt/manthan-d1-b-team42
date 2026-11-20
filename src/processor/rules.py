"""The 4 fraud rules as small Spark functions. Thresholds and severities come from rules.yml.

Input: one DataFrame of enriched payments = the card's recent history + the new batch, with
    event_id, card_token, merchant_id, amount, country, event_time, card_avg_30d,
    is_blacklisted, is_new  (True only for the new batch rows)
Output: alert rows (alert_id, rule_id, event_id, card_token, reason, severity, amount, event_time).

Why history: velocity and impossible travel look back in time per card. A new payment is
counted together with the card's earlier payments, even if those came in an earlier batch,
and even if this payment itself arrived late. Only is_new rows raise alerts, so a payment
is never alerted twice by the same rule.
"""

from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.processor.sinks import add_alert_id

ALERT_COLUMNS = ["rule_id", "event_id", "card_token", "reason", "severity", "amount", "event_time"]
MICROS_PER_MINUTE = 60_000_000
WINDOWED_RULES = ("velocity", "impossible_travel")


def _card_window(minutes):
    """The same card's payments in (event_time - minutes, event_time], ordered by event time.

    Java analogy: for each payment, a sliding view over that card's sorted payments.
    """
    return (
        Window.partitionBy("card_token")
        .orderBy(F.unix_micros("event_time"))
        .rangeBetween(-int(minutes * MICROS_PER_MINUTE) + 1, 0)
    )


def _alerts(hits, rule_id, cfg, reason):
    """Shape matching rows (new ones only) as alert rows."""
    return hits.filter(F.col("is_new")).select(
        F.lit(rule_id).alias("rule_id"),
        "event_id",
        "card_token",
        reason.alias("reason"),
        F.lit(cfg["severity"]).alias("severity"),
        "amount",
        "event_time",
    )


def velocity(events, cfg):
    """More than max_txn payments by one card within window_minutes."""
    hits = events.withColumn("_n", F.count("*").over(_card_window(cfg["window_minutes"]))).filter(
        F.col("_n") > cfg["max_txn"]
    )
    reason = F.format_string(
        f"%d payments in {cfg['window_minutes']} min (max {cfg['max_txn']})", F.col("_n")
    )
    return _alerts(hits, "velocity", cfg, reason)


def impossible_travel(events, cfg):
    """One card used in min_distinct_countries or more countries within window_minutes."""
    countries = F.array_sort(F.collect_set("country").over(_card_window(cfg["window_minutes"])))
    hits = events.withColumn("_countries", countries).filter(
        F.size("_countries") >= cfg["min_distinct_countries"]
    )
    reason = F.format_string(
        f"countries %s within {cfg['window_minutes']} min", F.array_join("_countries", ", ")
    )
    return _alerts(hits, "impossible_travel", cfg, reason)


def amount_spike(events, cfg):
    """Amount more than multiplier x the card's 30-day average. Cards without a baseline are skipped."""
    limit = F.col("card_avg_30d") * F.lit(cfg["multiplier"])
    hits = events.filter(F.col("card_avg_30d").isNotNull() & (F.col("amount") > limit))
    reason = F.format_string(
        f"amount %s > {cfg['multiplier']} x 30-day average %s",
        F.col("amount").cast("string"),
        F.col("card_avg_30d").cast("string"),
    )
    return _alerts(hits, "amount_spike", cfg, reason)


def merchant_blacklist(events, cfg):
    """Payment at a merchant listed in merchant_blacklist (silver set is_blacklisted)."""
    hits = events.filter(F.col("is_blacklisted"))
    reason = F.format_string("merchant %s is blacklisted", F.col("merchant_id"))
    return _alerts(hits, "merchant_blacklist", cfg, reason)


# Registry: rule name in rules.yml -> function (like a Map<String, Rule> in Java).
RULES = {
    "velocity": velocity,
    "impossible_travel": impossible_travel,
    "amount_spike": amount_spike,
    "merchant_blacklist": merchant_blacklist,
}


def run_rules(events, rules_cfg):
    """Run every enabled rule and return all alerts with alert_id, or None if all are disabled."""
    parts = [fn(events, rules_cfg[name]) for name, fn in RULES.items() if rules_cfg[name]["enabled"]]
    if not parts:
        return None
    return add_alert_id(reduce(lambda a, b: a.unionByName(b), parts))


def lookback_minutes(rules_cfg):
    """How much history the enabled time-window rules need (0 if none are enabled)."""
    windows = [
        rules_cfg[name]["window_minutes"] for name in WINDOWED_RULES if rules_cfg[name]["enabled"]
    ]
    return max(windows, default=0)
