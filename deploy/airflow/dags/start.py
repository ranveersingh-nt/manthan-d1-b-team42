import logging
import os
from datetime import datetime, timedelta

from airflow import DAG

# Works on Airflow 2.x and 3.x (operators moved to the standard provider in Airflow 3)
try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator


def say_hello():
    print("Hello from Team42! Airflow PythonOperator is working.")
    return "Success"


with DAG(
    dag_id="team42",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # 'schedule_interval' was removed in Airflow 3
    catchup=False,
) as dag:

    # FIX: needs 'echo', otherwise bash tries to run a command called "BashOperator"
    step_1 = BashOperator(
        task_id="init_task",
        bash_command='echo "BashOperator works!"',
    )

   

    # Runs src/dq/pre_dq_checks.py: Spark streams TOPIC_RAW from Kafka, applies the
    # Pre-DQ rules and stores accepted/rejected events in Postgres. It is a streaming job, so
    # the task keeps running (listening) until it is stopped. Config comes from the scheduler
    # container env (KAFKA_BOOTSTRAP, TOPIC_RAW, CHECKPOINT_DIR, PG_URL, PG_USER, PG_PASSWORD).
    pre_dq_check = BashOperator(
        task_id="pre_dq_check",
        bash_command="cd /opt/airflow && python -m src.dq.pre_dq_checks",
        execution_timeout=None,
    )

    step_1  >> pre_dq_check


# ---------------------------------------------------------------------------------------------
# Bronze -> Silver -> Gold -> final summary
#
# Flow (each step reads the previous step's table and is idempotent, so re-runs are safe):
#   payment_events_validated  (written by pre_dq_check above)
#     -> bronze  src/processor/bronze.py  -> payment_events_bronze   resumes from max(ingested_at)
#     -> silver  src/processor/silver.py  -> enriched_txn            business checks + card/merchant lookups
#     -> gold    src/processor/gold.py    -> alerts, agg_1min        fraud rules from config/rules.yml
#     -> publish_final_summary            -> row counts per layer + latest alerts in the task log / XCom
#
# A separate DAG because pre_dq_check is a streaming task that never finishes: anything placed
# after it in the team42 DAG would never start. max_active_runs=1 because silver and gold keep
# progress marks under CHECKPOINT_DIR/marks, which two overlapping runs must not update at once.
# All config comes from the scheduler container env (PG_URL, PG_USER, PG_PASSWORD, CHECKPOINT_DIR).
# ---------------------------------------------------------------------------------------------

LAYER_TABLES = (
    "payment_events_validated",
    "payment_events_invalidated",
    "payment_events_bronze",
    "enriched_txn",
    "alerts",
    "agg_1min",
)


def _pg_dsn():
    """Turn a JDBC URL (jdbc:postgresql://host:5432/db?x=y) into a psycopg2 URI."""
    url = os.environ["PG_URL"]
    if url.startswith("jdbc:"):
        url = url[len("jdbc:"):]
    # JDBC-only params (e.g. currentSchema) are rejected by libpq, so drop the query string
    return url.split("?", 1)[0]


def publish_final_summary():
    """Log and return (XCom) how many rows each layer holds, plus the 10 newest alerts."""
    import psycopg2  # imported here so DAG parsing stays fast (pip install psycopg2-binary)

    conn = psycopg2.connect(_pg_dsn(), user=os.getenv("PG_USER"), password=os.getenv("PG_PASSWORD"))
    try:
        with conn, conn.cursor() as cur:
            counts = {}
            for table in LAYER_TABLES:  # fixed names from the tuple above, never user input
                cur.execute(f"SELECT count(*) FROM {table}")
                counts[table] = cur.fetchone()[0]
            cur.execute(
                "SELECT rule_id, severity, card_token, amount, event_time "
                "FROM alerts ORDER BY created_at DESC LIMIT 10"
            )
            latest_alerts = [
                {
                    "rule_id": r[0],
                    "severity": r[1],
                    "card_token": r[2],
                    "amount": str(r[3]) if r[3] is not None else None,
                    # FIX: NULL event_time no longer crashes the task
                    "event_time": r[4].isoformat() if r[4] is not None else None,
                }
                for r in cur.fetchall()
            ]
    finally:
        conn.close()

    log = logging.getLogger("team42.final")
    for table, n in counts.items():
        log.info("%-28s %d rows", table, n)
    for a in latest_alerts:
        log.info("alert %s", a)
    return {"counts": counts, "latest_alerts": latest_alerts}


def _run_layer(module):
    """Run one layer once (--once = a single incremental load, then exit)."""
    return f"cd /opt/airflow && python -m {module} --once"


with DAG(
    dag_id="team42_bronze_silver_gold",
    start_date=datetime(2026, 11, 20),
    schedule="*/5 * * * *",  # 'schedule_interval' was removed in Airflow 3
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["streamguard", "medallion"],
) as medallion_dag:

    bronze = BashOperator(task_id="bronze", bash_command=_run_layer("src.processor.bronze"))
    silver = BashOperator(task_id="silver", bash_command=_run_layer("src.processor.silver"))
    gold = BashOperator(task_id="gold", bash_command=_run_layer("src.processor.gold"))
    final = PythonOperator(task_id="publish_final_summary", python_callable=publish_final_summary)

    bronze >> silver >> gold >> final