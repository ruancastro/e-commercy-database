from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.etl import ETLIncremental

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'incremental_load_olap',
    default_args=default_args,
    description='A DAG to incrementally load data into the OLAP database',
    schedule_interval=timedelta(days=1),  # Runs daily
    start_date=datetime(2025, 4, 22),
    catchup=False,
)

# Database connection URLs (replace with your actual database URLs)
OLTP_URL = "postgresql+psycopg2://oltp:ecommerce123@postgres_oltp:5432/ecommerce_oltp"
OLAP_URL = "postgresql+psycopg2://olap:ecommerce123@postgres_olap:5432/ecommerce_olap"

# Instantiate the ETLIncremental class
etl = ETLIncremental(oltp_url=OLTP_URL, olap_url=OLAP_URL)

# Define the task functions
def extract_task(**kwargs):
    """
    Task to extract incremental data from the OLTP database.
    Uses the last execution date to fetch only new or updated records.
    """
    # Get the last execution date from the context
    last_execution_date = kwargs['prev_ds'] if 'prev_ds' in kwargs else '2025-04-22 13:00:00'
    extracted_data = etl.extract(last_execution_date=last_execution_date)
    # Store the extracted data in XCom for the next task
    kwargs['ti'].xcom_push(key='extracted_data', value=extracted_data)

def transform_task(**kwargs):
    """
    Task to transform the extracted incremental data for the OLAP database.
    """
    # Pull the extracted data from XCom
    extracted_data = kwargs['ti'].xcom_pull(key='extracted_data', task_ids='extract_task')
    transformed_data = etl.transform(extracted_data)
    # Store the transformed data in XCom for the next task
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_data)

def load_task(**kwargs):
    """
    Task to load the transformed incremental data into the OLAP database.
    """
    # Pull the transformed data from XCom
    transformed_data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_task')
    etl.load(transformed_data)

# Define the tasks
extract = PythonOperator(
    task_id='extract_task',
    python_callable=extract_task,
    provide_context=True,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_task',
    python_callable=transform_task,
    provide_context=True,
    dag=dag,
)

load = PythonOperator(
    task_id='load_task',
    python_callable=load_task,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract >> transform >> load