"""Gate-level tests: accept/reject split, nothing lost, typed output, envelope, masking, replay."""

from decimal import Decimal

from pyspark.sql import functions as F

from src.dq.pre_dq_gate import run_pre_dq


def _mixed_batch(valid_event):
    return [
        valid_event(),  # 0 accepted
        valid_event(event_id="11111111-2222-4333-8444-555555555555", schema_version=...),  # 1 accepted
        None,  # 2 empty_message
        "not json",  # 3 malformed_json
        valid_event(currency=...),  # 4 missing_field
        valid_event(merchant_id="4111111111111111"),  # 5 pan_detected
        valid_event(amount="10.00", card_token="tok_BAD"),  # 6 two failures
    ]


def test_pre_dq_gate_splits_batch_without_losing_events(make_raw, dq_config, valid_event):
    raw = make_raw(_mixed_batch(valid_event))
    result = run_pre_dq(raw, dq_config)
    accepted = {r["src_offset"] for r in result.accepted.select("src_offset").collect()}
    rejected = {r["src_offset"] for r in result.rejected.select("src_offset").collect()}
    assert accepted == {0, 1}
    assert rejected == {2, 3, 4, 5, 6}
    assert accepted.isdisjoint(rejected)
    assert len(accepted) + len(rejected) == raw.count()


def test_pre_dq_gate_outputs_typed_accepted_events(make_raw, dq_config, valid_event):
    accepted = run_pre_dq(make_raw([valid_event(schema_version=...)]), dq_config).accepted
    # Format inside Spark (session tz = UTC); collect() would convert to the machine's local tz.
    row = accepted.withColumn("event_ts_utc", F.date_format("event_ts", "yyyy-MM-dd'T'HH:mm:ss.SSS")).collect()[0]
    assert dict(accepted.dtypes)["event_ts"] == "timestamp"
    assert row["event_ts_utc"] == "2026-11-20T10:15:30.123"
    assert row["amount"] == Decimal("42.50")
    assert row["schema_version"] == 1  # default applied
    assert row["merchant_category"] == "5411"
    assert row["src_topic"] == "payments.raw"


def test_pre_dq_gate_rejected_envelope_has_reasons(make_raw, dq_config, valid_event):
    raw = make_raw([valid_event(amount="10.00", card_token="tok_BAD")])
    row = run_pre_dq(raw, dq_config).rejected.collect()[0]
    assert row["dq_stage"] == "pre_dq"
    assert row["dq_rule"] == "invalid_format"
    assert row["dq_reason"] == "field 'card_token' does not match the required format"
    assert [(f["rule"], f["field"]) for f in row["dq_failures"]] == [
        ("invalid_format", "card_token"),
        ("invalid_type", "amount"),
    ]
    assert row["dq_ts"] is not None
    assert row["dq_config_version"] == 1
    assert row["event_id"] == valid_event()["event_id"]
    assert row["kafka_key"] == bytearray(b"tok_key")
    assert '"amount": "10.00"' in row["raw_payload"]  # original payload kept


def test_pre_dq_gate_masks_pan_in_rejected_payload_and_ids(make_raw, dq_config, valid_event):
    raw = make_raw([valid_event(merchant_id="4111 1111 1111 1111", event_id="5500000000000004")])
    row = run_pre_dq(raw, dq_config).rejected.collect()[0]
    assert "4111" not in row["raw_payload"]
    assert "5500000000000004" not in row["raw_payload"]
    assert "[REDACTED_PAN]" in row["raw_payload"]
    assert row["event_id"] == "[REDACTED_PAN]"


def test_pre_dq_gate_replay_gives_identical_split(make_raw, dq_config, valid_event):
    batch = _mixed_batch(valid_event)
    cols = ["src_offset", "dq_rule", "dq_reason", "dq_failures", "raw_payload"]

    def run_once():
        result = run_pre_dq(make_raw(batch), dq_config)
        acc = sorted(tuple(r) for r in result.accepted.collect())
        rej = sorted((r["src_offset"], str(r.asDict())) for r in result.rejected.select(*cols).collect())
        return acc, rej

    assert run_once() == run_once()


def test_pre_dq_gate_rejects_all_of_a_fully_bad_batch(make_raw, dq_config):
    result = run_pre_dq(make_raw([None, "[]", "{", b"\xff"]), dq_config)
    assert result.accepted.count() == 0
    assert sorted(r["dq_rule"] for r in result.rejected.collect()) == [
        "bad_encoding",
        "empty_message",
        "malformed_json",
        "not_json_object",
    ]
