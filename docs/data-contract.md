> **DRAFT – pending team agreement**

# Data contract: payment event

Topic: `payments.raw` (env `TOPIC_RAW`). Message key: `card_token` (UTF-8). Message value: one JSON object, UTF-8 encoded.

Producers must follow this contract. The Pre-DQ gate (`src/dq/`) enforces it and rejects any event that breaks it to `payments.dlq`.
Change this file first, then the producer, then `config/dq_rules.yaml`.

## Schema

| Field | JSON type | Required | Format / rule | Description |
|-------|-----------|----------|---------------|-------------|
| `event_id` | string | yes | UUID, `8-4-4-4-12` hex | Globally unique event ID. Used for dedup. |
| `card_token` | string | yes | `^tok_[0-9a-f]{16,32}$` | Tokenized card. Never a real or realistic card number. |
| `merchant_id` | string | yes | non-blank | Merchant identifier, e.g. `m_000123`. |
| `merchant_category` | string | yes | non-blank | Merchant category code (MCC), e.g. `"5411"`. Sent as a string. |
| `amount` | number | yes | JSON number, fits `decimal(18,2)` | Transaction amount in `currency` units. |
| `currency` | string | yes | non-blank | ISO-4217 code, e.g. `USD`. |
| `country` | string | yes | non-blank | ISO-3166 alpha-2 code, e.g. `US`. |
| `channel` | string | yes | non-blank | e.g. `ecom`, `pos`, `atm`, `moto`. |
| `event_ts` | string | yes | `YYYY-MM-DDTHH:MM:SS[.ffffff]Z` (UTC) | Time the payment happened (event time). |
| `status` | string | yes | non-blank | e.g. `approved`, `declined`, `pending`, `reversed`. |
| `schema_version` | integer | no | supported list in config (today: `1`) | Contract version. Defaults to `1` when absent. |

Extra fields are allowed and ignored by the gate.

## Pre-DQ gate (structural, per event)

Each event is accepted or rejected as a whole. A rejected event goes to `payments.dlq`, the Postgres table `dq_rejected_events` and the application log, with every failed rule and its reason.

| Rule code | Rejected when |
|-----------|---------------|
| `empty_message` | Kafka value is null, empty or whitespace only |
| `bad_encoding` | value is not valid UTF-8 |
| `malformed_json` | value is not valid JSON |
| `not_json_object` | value is valid JSON but not an object (array, scalar, `null`) |
| `missing_field` | a required field is absent |
| `null_or_blank_field` | a field is JSON `null`, or a blank string |
| `invalid_type` | a field has the wrong JSON type, or a value that can't be cast (e.g. `amount` overflows `decimal(18,2)`, `event_ts` is not a real UTC timestamp) |
| `invalid_format` | a field doesn't match its pattern (`event_id`, `card_token`) |
| `unsupported_schema_version` | `schema_version` is not in the supported list |
| `pan_detected` | the payload contains a 13–19 digit number or a grouped card-number pattern; the stored copy is masked |

Business checks (amount range, allowed currency, event time ≤ +24h) are **not** part of this gate. They run later on accepted events.

## Example event

```json
{
  "event_id": "3f2b8c1e-9d4a-4e6b-8f1a-2c3d4e5f6a7b",
  "card_token": "tok_9f86d081884c7d65",
  "merchant_id": "m_000123",
  "merchant_category": "5411",
  "amount": 42.50,
  "currency": "USD",
  "country": "US",
  "channel": "pos",
  "event_ts": "2026-11-20T10:15:30.123Z",
  "status": "approved",
  "schema_version": 1
}
```

## Dead-letter envelope (`payments.dlq`)

Key: the original Kafka key. Value (JSON):

```json
{
  "dq_stage": "pre_dq",
  "dq_rule": "missing_field",
  "dq_reason": "required field 'currency' is missing",
  "dq_failures": [
    {"rule": "missing_field", "field": "currency", "reason": "required field 'currency' is missing"},
    {"rule": "invalid_type", "field": "amount", "reason": "field 'amount' must be a JSON number that fits decimal(18,2)"}
  ],
  "dq_ts": "2026-11-20T10:15:31.002Z",
  "dq_config_version": 1,
  "event_id": "3f2b8c1e-9d4a-4e6b-8f1a-2c3d4e5f6a7b",
  "card_token": "tok_9f86d081884c7d65",
  "raw_payload": "{... original payload, card-number patterns masked ...}",
  "src_topic": "payments.raw",
  "src_partition": 3,
  "src_offset": 128734,
  "src_kafka_ts": "2026-11-20T10:15:30.456Z"
}
```

Reasons never contain field values, so the logs can't leak sensitive data.

## Versioning

- `schema_version` is an integer. Additive, optional fields don't bump it.
- Removing or renaming a field, changing a type, or making a field required bumps it.
- The gate accepts only versions listed in `config/dq_rules.yaml` → `pre_dq.supported_schema_versions`. Add the new version there before the producer starts sending it.
