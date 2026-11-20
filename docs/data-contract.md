> **DRAFT – pending team agreement**

# Data contract: payment event

The producer (`src/producer/`) and processor (`src/processor/`) must match this schema exactly.

## Example event

```json
{
  "event_id": "8f14e45f-ceea-4a1e-9b5e-6c1f0a2b3c4d",
  "card_token": "tok_3fa85f6457174562",
  "merchant_id": "m_00421",
  "merchant_category": "5411",
  "amount": 249.99,
  "currency": "INR",
  "country": "IN",
  "channel": "POS",
  "event_time": "2026-11-20T10:15:30Z",
  "status": "APPROVED"
}
```

## Fields

| Name | Type | Required | Rules |
|------|------|----------|-------|
| `event_id` | string (UUID) | Yes | Unique per event. Replays reuse the same id. |
| `card_token` | string | Yes | `tok_` followed by 16 hex chars. Never a real card number. |
| `merchant_id` | string | Yes | Merchant identifier, e.g. `m_00421`. |
| `merchant_category` | string | Yes | 4-digit string (MCC), e.g. `5411`. |
| `amount` | number | Yes | Greater than 0, 2 decimal places, below a configurable ceiling. |
| `currency` | string | Yes | From the allowed list: `INR`, `USD`, `EUR`, `GBP`, `AED`, `SGD`. |
| `country` | string | Yes | ISO 3166-1 alpha-2 code, e.g. `IN`. |
| `channel` | string | Yes | One of `POS`, `ECOM`, `ATM`. |
| `event_time` | string (timestamp) | Yes | ISO 8601 UTC ending in `Z`. Not more than 24h in the future. |
| `status` | string | Yes | One of `APPROVED`, `DECLINED`, `PENDING`. |

## Kafka topics

| Topic | Key | Partitions | Value |
|-------|-----|------------|-------|
| `payments.raw` | `card_token` | 6 | Payment event (schema above) |
| `payments.alerts` | `card_token` | 3 | Fraud alert |
| `payments.dlq` | `event_id` | 1 | Original message + reason |

### Why key = `card_token`

Keying by `card_token` keeps each card's events in order on one partition. The velocity and impossible-travel rules need that per-card ordering.
