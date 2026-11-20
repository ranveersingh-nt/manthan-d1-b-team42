---
paths:
  - "src/processor/**"
  - "deploy/sql/**"
---
# Processor and SQL rules (owner: Khushi)

- Parse events with an explicit `StructType` that matches `docs/data-contract.md`. Never infer the schema.
- Set a watermark on `event_ts`. Document the watermark and state-timeout choices in `docs/architecture.md`.
- Load fraud rules and thresholds from the Postgres rules table or config. Never hardcode them.
- Give every streaming query its own checkpoint path under `CHECKPOINT_DIR`.
- Write sinks with `foreachBatch` and idempotent upserts keyed on `event_id` or `(rule_id, event_id)`.
- Use ANSI-safe expressions (`try_cast`, `try_divide`). Avoid Python UDFs where a built-in function exists.
- Make SQL files re-runnable (`CREATE ... IF NOT EXISTS`, seeds with `ON CONFLICT`) and number them for run order (`001_schema.sql`, ...).
- Give each fraud rule tests for both the flagged and the not-flagged case.
