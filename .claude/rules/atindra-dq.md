---
paths:
  - "src/dq/**"
---
# Data quality rules (owner: Atindra)

- Validate events against `docs/data-contract.md`: required fields, types, enums and ranges.
- Send invalid events to `TOPIC_DLQ` with the original payload plus `dq_rule`, `dq_reason` and `dq_ts`.
- Never drop a record silently. Every event ends up in either the valid stream or the DLQ.
- Give each DQ rule a name and keep it in a registry, one function per rule, so each can be tested on its own.
- Emit health metrics per micro-batch (counts in, valid and invalid; null rates; lag) to Postgres through an idempotent upsert.
- Keep tolerance thresholds (null %, late %) in config, not code.
- Use `try_cast` for type checks (ANSI mode) and treat a failed cast as a DQ violation.
