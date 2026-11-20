"""One pass case and one or more fail cases per Pre-DQ rule, including boundary values."""

import pytest

from src.dq.pre_dq_gate import annotate
from src.dq.pre_dq_rules import RULE_NAMES

REQUIRED_FIELDS = [
    "event_id",
    "card_token",
    "merchant_id",
    "merchant_category",
    "amount",
    "currency",
    "country",
    "channel",
    "event_ts",
    "status",
]


@pytest.fixture
def failures_for(make_raw, dq_config):
    """Run the rules on one payload and return [(rule, field), ...] in report order."""

    def _run(payload):
        row = annotate(make_raw([payload]), dq_config.pre_dq).select("dq_failures").collect()[0]
        return [(f["rule"], f["field"]) for f in row["dq_failures"]]

    return _run


def test_registry_has_unique_rule_names():
    assert len(RULE_NAMES) == len(set(RULE_NAMES))


def test_valid_event_passes_all_rules(failures_for, valid_event):
    assert failures_for(valid_event()) == []


def test_valid_event_without_optional_schema_version_passes(failures_for, valid_event):
    assert failures_for(valid_event(schema_version=...)) == []


# --------------------------------------------------------------------------- empty_message


@pytest.mark.parametrize("payload", [None, b"", "   ", "\n\t"], ids=["null", "empty", "spaces", "whitespace"])
def test_empty_message_rule_rejects_empty_value(failures_for, payload):
    assert failures_for(payload) == [("empty_message", None)]


# --------------------------------------------------------------------------- bad_encoding


def test_bad_encoding_rule_rejects_invalid_utf8(failures_for):
    assert failures_for(b'{"event_id": "\xff\xfe"}') == [("bad_encoding", None)]


def test_bad_encoding_rule_accepts_non_ascii_utf8(failures_for, valid_event):
    assert failures_for(valid_event(merchant_id="m_café_123")) == []


# --------------------------------------------------------------------------- malformed_json


@pytest.mark.parametrize("payload", ['{"event_id": ', "not json", "{'single': 'quotes'}", '{"a":1,}'])
def test_malformed_json_rule_rejects_unparseable_value(failures_for, payload):
    assert failures_for(payload) == [("malformed_json", None)]


# --------------------------------------------------------------------------- not_json_object


@pytest.mark.parametrize("payload", ["[1, 2]", "42", "null", '"text"', "true"])
def test_not_json_object_rule_rejects_non_object_json(failures_for, payload):
    assert failures_for(payload) == [("not_json_object", None)]


# --------------------------------------------------------------------------- missing_field


@pytest.mark.parametrize("field", REQUIRED_FIELDS)
def test_missing_field_rule_rejects_absent_required_field(failures_for, valid_event, field):
    assert failures_for(valid_event(**{field: ...})) == [("missing_field", field)]


def test_missing_field_rule_reports_every_missing_field(failures_for):
    rules = failures_for({})
    assert rules == [("missing_field", f) for f in REQUIRED_FIELDS]


# --------------------------------------------------------------------------- null_or_blank_field


@pytest.mark.parametrize(
    "field,value",
    [
        ("currency", None),
        ("currency", ""),
        ("currency", "   "),
        ("currency", "\t\n"),
        ("amount", None),
        ("event_ts", ""),
        ("schema_version", None),
    ],
)
def test_null_or_blank_field_rule_rejects_null_or_blank(failures_for, valid_event, field, value):
    assert failures_for(valid_event(**{field: value})) == [("null_or_blank_field", field)]


# --------------------------------------------------------------------------- invalid_type


@pytest.mark.parametrize(
    "field,value",
    [
        ("amount", "42.50"),  # numeric string is not a JSON number
        ("amount", True),
        ("amount", {"value": 1}),
        ("amount", 1e17),  # overflows decimal(18,2)
        ("merchant_category", 5411),  # MCC must be a string
        ("currency", ["USD"]),
        ("event_ts", 1763633730),  # epoch is not ISO-8601
        ("event_ts", "2026-11-20 10:15:30"),  # no T / Z
        ("event_ts", "2026-11-20T10:15:30+05:30"),  # not UTC Z
        ("event_ts", "2026-02-30T10:15:30Z"),  # impossible date
        ("event_ts", "2026-11-20T25:00:00Z"),  # impossible hour
        ("schema_version", "1"),
        ("schema_version", 1.5),
    ],
)
def test_invalid_type_rule_rejects_wrong_type(failures_for, valid_event, field, value):
    assert failures_for(valid_event(**{field: value})) == [("invalid_type", field)]


@pytest.mark.parametrize(
    "field,value",
    [
        ("amount", 0.01),
        ("amount", 42),
        ("amount", 99999999999.99),  # large but < 13 integer digits (13+ look like a PAN)
        ("event_ts", "2026-11-20T10:15:30Z"),
        ("event_ts", "2026-11-20T10:15:30.123456Z"),
        ("event_ts", "2028-02-29T00:00:00Z"),  # leap day
    ],
)
def test_invalid_type_rule_accepts_valid_boundary_values(failures_for, valid_event, field, value):
    assert failures_for(valid_event(**{field: value})) == []


def test_invalid_type_rule_leaves_business_ranges_to_later_stage(failures_for, valid_event):
    # Negative or zero amounts and unknown currencies are use-case DQ checks, not Pre-DQ.
    assert failures_for(valid_event(amount=-5, currency="XXX")) == []


# --------------------------------------------------------------------------- invalid_format


@pytest.mark.parametrize(
    "field,value",
    [
        ("event_id", "abc"),
        ("event_id", "3f2b8c1e9d4a4e6b8f1a2c3d4e5f6a7b"),  # no dashes
        ("card_token", "tok_XYZ"),
        ("card_token", "tok_9F86D081884C7D65"),  # upper-case hex
        ("card_token", "tok_9f86d081"),  # too short
        ("card_token", "9f86d081884c7d65"),  # no prefix
    ],
)
def test_invalid_format_rule_rejects_bad_identifier(failures_for, valid_event, field, value):
    assert failures_for(valid_event(**{field: value})) == [("invalid_format", field)]


# --------------------------------------------------------------------------- unsupported_schema_version


def test_unsupported_schema_version_rule_rejects_unknown_version(failures_for, valid_event):
    assert failures_for(valid_event(schema_version=2)) == [("unsupported_schema_version", "schema_version")]


# --------------------------------------------------------------------------- pan_detected


@pytest.mark.parametrize(
    "field,value",
    [
        ("merchant_id", "4111111111111111"),
        ("merchant_id", "m_x 4111 1111 1111 1111"),
        ("merchant_id", "4111-1111-1111-1111"),
        ("merchant_id", "3782 822463 10005"),  # 4-6-5 grouping
        ("note", "card 5500000000000004 used"),  # extra fields are scanned too
    ],
)
def test_pan_detected_rule_rejects_card_number_like_value(failures_for, valid_event, field, value):
    assert ("pan_detected", None) in failures_for(valid_event(**{field: value}))


@pytest.mark.parametrize(
    "field,value",
    [
        ("card_token", "tok_1234567890123456"),  # digits glued to the token prefix
        ("event_id", "12345678-1234-4234-8234-123456789012"),  # numeric-looking UUID
        ("merchant_id", "m_123456789012"),  # 12 digits
        ("amount", 123456789012.34),
    ],
)
def test_pan_detected_rule_ignores_non_card_digits(failures_for, valid_event, field, value):
    assert ("pan_detected", None) not in failures_for(valid_event(**{field: value}))


# --------------------------------------------------------------------------- combinations


def test_pre_dq_reports_all_failures_in_order(failures_for, valid_event):
    event = valid_event(currency=..., amount="42.50", card_token="tok_bad", schema_version=7)
    assert failures_for(event) == [
        ("invalid_format", "card_token"),
        ("invalid_type", "amount"),
        ("missing_field", "currency"),
        ("unsupported_schema_version", "schema_version"),
    ]


def test_fatal_rule_stops_field_rules(failures_for):
    assert failures_for('{"event_id": 1') == [("malformed_json", None)]
