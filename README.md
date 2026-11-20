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
4. Create a `.env` file in the repo root with the variables below. Never commit `.env` (it is in `.gitignore`).
5. Start the stack: `docker compose -f deploy/docker-compose.yaml up -d` (Vibha adds the compose file).
6. Run the tests: `pytest`.

## Environment variables (`.env`)

| Variable | Required | Example | Used by |
|----------|----------|---------|---------|
| `PG_URL` | Yes | `jdbc:postgresql://localhost:5432/team42_db` | DQ, processor |
| `PG_USER` | Yes | `changeme` | DQ, processor |
| `PG_PASSWORD` | Yes | `changeme` | DQ, processor |
| `KAFKA_BOOTSTRAP` | Yes | `localhost:9092` | DQ |
| `CHECKPOINT_DIR` | Yes | `./checkpoints` | DQ, processor |
| `TOPIC_RAW` | No | `payments.raw` | DQ |
| `RULES_CONFIG_PATH` | No | `src/processor/config/rules.yml` | processor |
| `BRONZE_TABLE` | No | `payment_events_bronze` | processor (bronze) |
| `BRONZE_POLL_SECONDS` | No | `5` | processor (bronze) |
| `BRONZE_OVERLAP_SECONDS` | No | `30` | processor (bronze) |
| `SILVER_POLL_SECONDS` | No | `5` | processor (silver) |
| `SILVER_OVERLAP_SECONDS` | No | `30` | processor (silver) |
| `SILVER_CHECKS_PATH` | No | `src/processor/config/silver_checks.yml` | processor (silver) |
| `GOLD_POLL_SECONDS` | No | `5` | processor (gold) |
| `GOLD_OVERLAP_SECONDS` | No | `30` | processor (gold) |

See `docs/runbook.md` for operations and `docs/branching-strategy.md` for the PR workflow.
