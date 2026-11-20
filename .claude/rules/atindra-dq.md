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

## Prompt log (DQ work)

Format: date · prompt summary · outcome. Newest last.

| Date | Prompt | Outcome |
|------|--------|---------|
| 2026-11-20 | Analyse the StreamGuard use case and plan the data quality checks for incoming data. | Plan drafted; scope narrowed to a Pre-DQ gate, separate from fraud rules. |
| 2026-11-20 | Clarified: Pre-DQ = strict accept/reject on incoming data (like CSV pre-checks), separate from use-case DQ checks. | Two layers agreed: Pre-DQ gate (structural) then use-case DQ (amount, currency, +24h). |
| 2026-11-20 | Choose the unit of accept/reject. | Option A: each event is accepted or rejected on its own. Rejected events must be logged with reasons. |
| 2026-11-20 | Only write code; the owner does all git commits and pushes. | Agreed. No git operations by Claude. |
| 2026-11-20 | Revert `src/dq/` and work step by step; follow this file instead of CLAUDE.md. | `src/dq/` reset to `.gitkeep`. |
| 2026-11-20 | Write a Python file to fetch data from Kafka and apply the Pre-DQ checks only. | `src/dq/pre_dq_checks.py`: Kafka read + 10 rules (empty, encoding, JSON, object, missing, null/blank, type, format, PAN, schema version). Smoke-tested on 9 sample events. |
| 2026-11-20 | Explain the flow; what to ask teammates for. | Flow explained; requests drafted for producer, orchestration and processor owners. |
| 2026-11-20 | List all Pre-DQ checks; are they enough for SQL? | Gaps found (NUL chars, lengths, amount scale, case variants, UUID case, key mismatch, MCC). Proposed as checks 1–10, pending approval. |
| 2026-11-20 | SQL to create the Postgres table for accepted events. | DDL for `payment_events_validated` (PK event_id UUID, unique source offset, indexes). |
| 2026-11-20 | Store accepted events into `payment_events_validated`; log prompts in this file. | `foreachBatch` sink: ACCEPTED rows inserted with `ON CONFLICT DO NOTHING` in one transaction per batch; REJECTED rows logged with reasons; batch summary logged. Fixed: missing `schema_version` now defaults to 1 (NULL broke the NOT NULL column). |
| 2026-11-20 | Store rejected events in `payment_events_invalidated` with the reason in a `reason` column. | REJECTED rows inserted in the same transaction as ACCEPTED rows, keyed on source offset with `ON CONFLICT DO NOTHING`. `reason` = all failure reasons joined with `; `; also `dq_rule`, `dq_failures` (jsonb), masked `raw_payload`. Card numbers masked, NUL escaped, lengths capped before storing. |
| 2026-11-20 | Don't run tests; just write the code. | Agreed. Code is written and reviewed by reading only. |
| 2026-11-20 | Create a flow diagram of how data enters and moves through the Pre-DQ check file. | `docs/pre-dq-flow.md`: Mermaid flowchart (fetch → prepare → rules → decision → one-transaction store → logs) + stage table + guarantees. |
| 2026-11-20 | Change the Kafka topic name to `ray_payments` in the code. | `read_raw_events` now defaults to `ray_payments` (`DEFAULT_RAW_TOPIC`); env `TOPIC_RAW` still overrides it. |
