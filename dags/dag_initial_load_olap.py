from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.etl import ETLInitial  # Import the ETLInitial class

# Default DAG configuration
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 22),
    "retries": 1,
}

# Database connection URLs
OLTP_URL = "postgresql+psycopg2://oltp:ecommerce123@postgres_oltp:5432/ecommerce_oltp"
OLAP_URL = "postgresql+psycopg2://olap:ecommerce123@postgres_olap:5432/ecommerce_olap"
# OLAP_URL = "postgresql+psycopg2://olap:ecommerce123@localhost:5434/ecommerce_olap"  # local

etl = ETLInitial(oltp_url=OLTP_URL, olap_url=OLAP_URL)


def extract_task():
    return etl.extract()


def transform_task(**context):
    extracted_data = context["task_instance"].xcom_pull(task_ids="extract_task")
    return etl.transform(extracted_data)


def load_task(**context):
    transformed_data = context["task_instance"].xcom_pull(task_ids="transform_task")
    etl.load(transformed_data)


with DAG(
    "initial_load_olap",
    default_args=default_args,
    description="DAG for initial load of the OLAP database",
    schedule_interval=None,  # Run manually
    catchup=False,
) as dag:
    extract = PythonOperator(
        task_id="extract_task",
        python_callable=extract_task,
    )

    transform = PythonOperator(
        task_id="transform_task",
        python_callable=transform_task,
        provide_context=True,
    )

    load = PythonOperator(
        task_id="load_task",
        python_callable=load_task,
        provide_context=True,
    )

    extract >> transform >> load
