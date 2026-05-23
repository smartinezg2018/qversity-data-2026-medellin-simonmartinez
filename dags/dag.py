from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import boto3
from botocore import UNSIGNED
from botocore.config import Config
import json

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

def download_from_s3():
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    s3.download_file(
        Bucket='qversity-raw-public-data',
        Key='fintech_banking_dataset.json',
        Filename='data/raw/fintech_banking_dataset.json'
    )

download_task = PythonOperator(
    task_id='download_file_from_s3',
    python_callable=download_from_s3,
    dag=dag,
)

# ---------------------------------------------------------------
# Task 2: Load JSON records into bronze.raw_fintech_data
# ---------------------------------------------------------------

def load_to_bronze():
    LOCAL_PATH = 'data/raw/fintech_banking_dataset.json'
    with open(LOCAL_PATH, "r") as f:
        records = json.load(f)

    hook = PostgresHook(postgres_conn_id="postgres_default")

    # Reset table (idempotent) then bulk-insert
    hook.run("TRUNCATE TABLE bronze.raw_fintech_data;")

    now = datetime.now()
    rows = [(json.dumps(record), now) for record in records]
    hook.insert_rows(
        table='bronze.raw_fintech_data',
        rows=rows,
        target_fields=['data', 'load_timestamp']
    )
    print(f"Successfully loaded {len(rows)} records into bronze.raw_fintech_data.")

load_task = PythonOperator(
    task_id='load_file_to_postgre',
    python_callable=load_to_bronze,
    dag=dag,
)
 

# --------bronze------------------------

# ---------------------------------------------------------------
# Task 3: PySpark — Silver staging (flatten arrays, dedup, JDBC write)
# Runs inside the Airflow container; spark/ is mounted at /opt/airflow/spark
# ---------------------------------------------------------------
run_spark_silver = BashOperator(
    task_id="run_spark_silver",
    bash_command="python /opt/airflow/spark/script.py",
    dag=dag,
)


# dbt runs in the dedicated dbt container (qversity_dbt); Airflow uses docker exec
# via the mounted Docker socket (/var/run/docker.sock).
DBT_CONTAINER = "qversity_dbt"
DBT_DIR = "/dbt"


def _dbt_exec(subcommand: str) -> str:
    return (
        f"docker exec {DBT_CONTAINER} "
        f"bash -c 'cd {DBT_DIR} && dbt {subcommand}'"
    )


dbt_seed = BashOperator(
    task_id="dbt_seed",
    bash_command=_dbt_exec("seed"),
    dag=dag,
)

dbt_silver_models = BashOperator(
    task_id="dbt_silver_models",
    bash_command=_dbt_exec("run --select silver"),
    dag=dag,
)

dbt_silver_test = BashOperator(
    task_id="dbt_silver_test",
    bash_command=_dbt_exec("test --select silver"),
    dag=dag,
)

dbt_gold_models = BashOperator(
    task_id="dbt_gold_models",
    bash_command=_dbt_exec("run --select gold"),
    dag=dag,
)

dbt_gold_test = BashOperator(
    task_id="dbt_gold_test",
    bash_command=_dbt_exec("test --select gold"),
    dag=dag,
)

# setup_task >> download_task >> load_task >> run_spark_silver >> dbt_seed >> dbt_models >> dbt_test
setup_task >> download_task >>load_task >> run_spark_silver >> dbt_seed >> dbt_silver_models >> dbt_silver_test >> dbt_gold_models >> dbt_gold_test
