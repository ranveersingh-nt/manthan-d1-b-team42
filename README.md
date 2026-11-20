# AI Manthan: StreamGuard – Real-Time Payment Fraud Signals

StreamGuard takes in tokenized payment events from Kafka and scores them with configurable fraud rules in Spark Structured Streaming 4.1.
Alerts go to Kafka and PostgreSQL 18. Bad events go to a dead-letter topic, and data-quality health metrics are tracked.
Airflow runs the daily reports and Superset shows the dashboards. Everything runs in Docker.

## Repository layout

| Path | Purpose | Owner |
|------|---------|-------|
| `src/producer/` | Emits synthetic payment events to Kafka | Ranveer, Vibha |
| `src/processor/` | Spark Structured Streaming, fraud rules, sinks | Khushi |
| `src/dq/` | DQ rules, dead-letter topic, health metrics | Atindra |
| `src/dags/` | Airflow DAGs (daily precision report, card stats refresh) | Vibha |
| `deploy/` | Docker Compose, Kafka broker, Airflow and Superset images | Vibha |
| `deploy/sql/` | Postgres schemas, tables, rules seed | Khushi |
| `tests/` | Unit and integration tests | All |
| `docs/` | Architecture, ADRs, data contract, runbook, prompt log | All |
| `.claude/rules/` | Per-owner Claude Code rules | Each owner |

## Setup

1. Clone the repo and check out `main-dev`.
2. Create a virtual env with `python -m venv .venv`, then activate it.
3. Install dependencies: `pip install -r requirements.txt`.
4. Copy `.env.example` to `.env` and fill in local values. Never commit `.env`.
5. Start the stack: `docker compose -f deploy/docker-compose.yaml up -d` (Vibha adds the compose file).
6. Run the tests: `pytest`.

See `docs/runbook.md` for operations and `docs/branching-strategy.md` for the PR workflow.
