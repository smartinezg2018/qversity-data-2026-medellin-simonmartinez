from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def hello_qversity():
    print("Hello from Qversity v2!")
    print("This is a placeholder DAG.")
    print("Replace this with your actual pipeline logic.")
    return "Pipeline started"


default_args = {
    "owner": "qversity",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "qversity_fintech_pipeline",
    default_args=default_args,
    description="Qversity v2 Fintech/Banking ELT Pipeline",
    schedule_interval=None,
    catchup=False,
    tags=["qversity", "fintech"],
)

# ---------------------------------------------------------------
# Task 0: Setup — Create schemas and base table (idempotent DDL)
# ---------------------------------------------------------------

def setup_schemas():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    hook.run("""
        CREATE SCHEMA IF NOT EXISTS bronze;
        CREATE SCHEMA IF NOT EXISTS silver;
        CREATE SCHEMA IF NOT EXISTS gold;

        CREATE TABLE IF NOT EXISTS bronze.raw_fintech_data (
            id             SERIAL PRIMARY KEY,
            data           JSONB NOT NULL,
            load_timestamp TIMESTAMP DEFAULT NOW()
        );
    """)
    print("Schemas and base tables ready.")

setup_task = PythonOperator(
    task_id='setup_schemas',
    python_callable=setup_schemas,
    dag=dag,
)

# ---------------------------------------------------------------
# Task 1: Placeholder - Download JSON from S3 and load to Bronze
# ---------------------------------------------------------------
hello_task = PythonOperator(
    task_id="hello_qversity",
    python_callable=hello_qversity,
    dag=dag,
)

