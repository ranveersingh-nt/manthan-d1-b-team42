"""Reject logger tests: log lines carry reasons but no payload; Postgres upsert is idempotent."""

import json
import logging

import pytest

from src.dq import reject_logger
from src.dq.pre_dq_gate import run_pre_dq
from src.dq.reject_logger import (
    UPSERT_COLUMNS,
    UPSERT_SQL,
    RejectedEventSink,
    build_log_record,
    to_dlq_frame,
    upsert_rejected,
)


class FakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConn:
    def __init__(self):
        self.commits = 0
        self.closed = False
        self.rolled_back = False

    def cursor(self):
        return FakeCursor()

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


@pytest.fixture
def captured_upserts(monkeypatch):
    """Capture rows passed to psycopg2 execute_values instead of hitting Postgres."""
    calls = []

    def fake_execute_values(cursor, sql, params, template=None, page_size=None):
        calls.append({"sql": sql, "params": params, "template": template})

    import psycopg2.extras

    monkeypatch.setattr(psycopg2.extras, "execute_values", fake_execute_values)
    return calls


def test_upsert_sql_is_idempotent_on_source_offset():
    assert "ON CONFLICT (src_topic, src_partition, src_offset) DO UPDATE" in UPSERT_SQL


def test_upsert_rejected_drops_repeated_offsets_in_chunk(captured_upserts):
    row = {c: None for c in UPSERT_COLUMNS} | {"src_topic": "payments.raw", "src_partition": 0, "src_offset": 7}
    upsert_rejected(FakeCursor(), [row, dict(row)])
    assert len(captured_upserts[0]["params"]) == 1
    assert "%s::jsonb" in captured_upserts[0]["template"]


def test_build_log_record_has_reasons_and_no_payload():
    row = {
        "src_topic": "payments.raw",
        "src_partition": 1,
        "src_offset": 42,
        "event_id": "e1",
        "card_token": "tok_9f86d081884c7d65",
        "dq_rule": "missing_field",
        "dq_reason": "required field 'currency' is missing",
        "dq_failures": json.dumps(
            [{"rule": "missing_field", "field": "currency", "reason": "required field 'currency' is missing"}]
        ),
        "raw_payload": '{"secret": "do-not-log"}',
    }
    record = build_log_record(row, batch_id=3)
    assert record["dq_rule"] == "missing_field"
    assert record["dq_failures"][0]["field"] == "currency"
    assert record["src_offset"] == 42
    assert "raw_payload" not in record
    assert "do-not-log" not in json.dumps(record)


def test_to_dlq_frame_keeps_key_and_all_envelope_fields(make_raw, dq_config):
    rejected = run_pre_dq(make_raw(["not json"]), dq_config).rejected
    row = to_dlq_frame(rejected).collect()[0]
    assert row["key"] == bytearray(b"tok_key")
    value = json.loads(row["value"])
    assert value["dq_rule"] == "malformed_json"
    assert value["raw_payload"] == "not json"
    assert value["src_offset"] == 0
    assert set(reject_logger.DLQ_VALUE_FIELDS) <= set(value)
    assert value["event_id"] is None  # unknown for malformed JSON, but the key is still present


def test_reject_sink_logs_and_upserts_every_rejected_event(make_raw, dq_config, valid_event, captured_upserts, caplog):
    rejected = run_pre_dq(make_raw([valid_event(), None, "not json", valid_event(currency=...)]), dq_config).rejected
    conn = FakeConn()
    sink = RejectedEventSink(dq_config, pg_connect=lambda: conn, write_dlq=False)

    with caplog.at_level(logging.INFO, logger="streamguard.dq.pre_dq"):
        sink(rejected, batch_id=9)

    upserted = [p for call in captured_upserts for p in call["params"]]
    assert sorted(p[UPSERT_COLUMNS.index("src_offset")] for p in upserted) == [1, 2, 3]
    assert conn.commits >= 1 and conn.closed

    records = [json.loads(r.getMessage()) for r in caplog.records]
    rejects = [r for r in records if r["event"] == "pre_dq_reject"]
    summary = [r for r in records if r["event"] == "pre_dq_reject_batch"][0]
    assert sorted(r["dq_rule"] for r in rejects) == ["empty_message", "malformed_json", "missing_field"]
    assert all(r["dq_reason"] for r in rejects)
    assert summary["rejected"] == 3
    assert summary["by_rule"] == {"empty_message": 1, "malformed_json": 1, "missing_field": 1}


def test_reject_sink_skips_empty_batch(make_raw, dq_config, valid_event, captured_upserts):
    rejected = run_pre_dq(make_raw([valid_event()]), dq_config).rejected
    sink = RejectedEventSink(dq_config, pg_connect=lambda: pytest.fail("should not connect"), write_dlq=False)
    sink(rejected, batch_id=1)
    assert captured_upserts == []


def test_reject_sink_rolls_back_and_raises_on_db_error(make_raw, dq_config, monkeypatch):
    rejected = run_pre_dq(make_raw(["not json"]), dq_config).rejected
    conn = FakeConn()

    def boom(*args, **kwargs):
        raise RuntimeError("db down")

    monkeypatch.setattr(reject_logger, "upsert_rejected", boom)
    sink = RejectedEventSink(dq_config, pg_connect=lambda: conn, write_dlq=False)
    with pytest.raises(RuntimeError, match="db down"):
        sink(rejected, batch_id=2)
    assert conn.rolled_back and conn.closed


def test_reject_sink_requires_kafka_settings_for_dlq(dq_config, monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP", raising=False)
    monkeypatch.delenv("TOPIC_DLQ", raising=False)
    with pytest.raises(RuntimeError, match="KAFKA_BOOTSTRAP"):
        RejectedEventSink(dq_config)
