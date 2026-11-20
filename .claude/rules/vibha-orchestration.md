---
paths:
  - "deploy/**"
  - "src/dags/**"
---
# Orchestration rules (owner: Vibha)

- Every service in `docker-compose.yaml` reads config from `.env` through `env_file` or `${VAR}`. No inline secrets.
- Pin image versions (Kafka, Postgres 18, Spark 4.1, Airflow, Superset). Never use `latest`.
- Create the Kafka topics `payments.raw`, `payments.alerts` and `payments.dlq` explicitly at startup and document their partition counts.
- Give every service a healthcheck and use `depends_on` with `condition: service_healthy`.
- Keep DAGs idempotent and safe to re-run: set `catchup` explicitly and key tasks on the logical date.
- DAGs read connections and variables from env or the Airflow connections store. Never hardcode them.
- Mount checkpoint and data volumes at paths taken from env (`CHECKPOINT_DIR`). Never commit them.
