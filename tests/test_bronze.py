from datetime import datetime, timezone

import pytest

from src.processor.bronze import bronze_table, source_query
from src.processor.marks import read_mark, save_mark
from src.processor.sinks import jdbc_url

MARK = datetime(2026, 11, 20, 10, 16, 10, tzinfo=timezone.utc)


def test_empty_bronze_reads_all_validated_rows():
    sql = source_query(None, overlap_seconds=30)

    assert sql.startswith("SELECT event_id, ")
    assert "FROM payment_events_validated" in sql
    assert "WHERE" not in sql


def test_resume_point_rereads_overlap_window():
    sql = source_query(MARK, overlap_seconds=30)

    assert "WHERE ingested_at > '2026-11-20T10:15:40+00:00'::timestamptz" in sql


def test_bronze_table_defaults_to_payment_events_bronze(monkeypatch):
    monkeypatch.delenv("BRONZE_TABLE", raising=False)

    assert bronze_table() == "payment_events_bronze"


def test_unsafe_bronze_table_name_is_rejected(monkeypatch):
    monkeypatch.setenv("BRONZE_TABLE", "bronze; DROP TABLE alerts")

    with pytest.raises(ValueError, match="BRONZE_TABLE"):
        bronze_table()


def test_libpq_pg_url_gets_jdbc_prefix(monkeypatch):
    monkeypatch.setenv("PG_URL", "postgresql://localhost:5432/team42_db")

    assert jdbc_url() == "jdbc:postgresql://localhost:5432/team42_db"


def test_missing_pg_url_is_rejected(monkeypatch):
    monkeypatch.delenv("PG_URL", raising=False)

    with pytest.raises(ValueError, match="PG_URL"):
        jdbc_url()


def test_missing_mark_returns_none(tmp_path, monkeypatch):
    monkeypatch.setenv("CHECKPOINT_DIR", str(tmp_path))

    assert read_mark("silver_bronze_loaded_at") is None


def test_saved_mark_is_read_back(tmp_path, monkeypatch):
    monkeypatch.setenv("CHECKPOINT_DIR", str(tmp_path))

    save_mark("silver_bronze_loaded_at", MARK)

    assert read_mark("silver_bronze_loaded_at") == MARK
