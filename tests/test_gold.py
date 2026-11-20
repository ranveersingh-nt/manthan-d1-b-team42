"""Gold query tests (plain strings, no Spark or database needed)."""

from datetime import datetime, timezone

from src.processor.gold import agg_query, context_query, new_rows_query

MARK = datetime(2026, 11, 20, 10, 16, 10, tzinfo=timezone.utc)
MIN_ET = "2026-11-20T10:15:30.000000Z"
MAX_ET = "2026-11-20T10:17:05.000000Z"


def test_first_run_judges_all_enriched_rows():
    assert "WHERE" not in new_rows_query(None, overlap_seconds=30)


def test_next_run_rereads_overlap_window():
    sql = new_rows_query(MARK, overlap_seconds=30)

    assert "WHERE processed_at >= '2026-11-20T10:15:40+00:00'::timestamptz" in sql


def test_context_reaches_back_by_the_longest_rule_window():
    sql = context_query(MIN_ET, MAX_ET, lookback=30)

    assert f"event_time >= '{MIN_ET}'::timestamptz - interval '30 minutes'" in sql
    assert f"event_time <= '{MAX_ET}'::timestamptz" in sql


def test_agg_recounts_whole_minutes_from_first_to_last_new_event():
    sql = agg_query(MIN_ET, MAX_ET)

    assert f"date_trunc('minute', '{MIN_ET}'::timestamptz)" in sql
    assert f"date_trunc('minute', '{MAX_ET}'::timestamptz) + interval '1 minute'" in sql
    assert "GROUP BY 1, 2, 3" in sql  # one row per (minute, category, country), as the upsert needs
