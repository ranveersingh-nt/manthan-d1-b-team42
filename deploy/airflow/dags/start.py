from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def say_hello():
    print("Hello from Team42! Airflow PythonOperator is working.")
    return "Success"

with DAG(
    dag_id="team42",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    step_1 = BashOperator(
        task_id="print_date_bash",
        bash_command="date && echo 'BashOperator works!'",
    )

    step_2 = PythonOperator(
        task_id="run_python_function",
        python_callable=say_hello,
    )

    step_3 = BashOperator(
        task_id="finish_pipeline",
        bash_command="echo 'Sample DAG finished successfully!'",
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

    step_1 >> step_2 >> [step_3, pre_dq_check]
