# CLAUDE.md: StreamGuard team-wide rules

These are shared rules only. Area-specific rules live in `.claude/rules/`. Change this file only through a PR to `main-dev`.

## Workflow
- Plan first: outline the approach before writing code, then implement.
- Log significant prompts and their outcomes in `docs/prompt-log.md`.
- Branch from `main-dev` as `feature/<topic>` or `fix/<topic>`. Merge only through a PR with at least 1 review.
- Commit prefixes: `feat:`, `fix:`, `test:`, `docs:`, `ci:`, `chore:`.

## Code
- Write all paths relative to the repo root. Never use absolute machine paths such as `C:\...`.
- No secrets in code. Read config with `os.getenv` (backed by `.env`, which is never committed).
- Use card tokens only. Never use, generate or log real card numbers (PANs).
- Keep fraud thresholds in config (env or the rules table). Never hardcode them.
- Make every Postgres write an idempotent upsert (`INSERT ... ON CONFLICT ... DO UPDATE`).
- PySpark 4.1 runs in ANSI mode. Use `try_cast` and the other `try_*` functions instead of relying on silent nulls.
- Use snake_case for file, function, variable, column and event field names.

## Tests
- Name each test after the rule and the expected result, e.g. `test_velocity_rule_flags_five_txns_in_60s`.
