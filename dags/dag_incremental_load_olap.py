from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.etl import ETLIncremental

# Define the default arguments for the DAG
# Configuração do DAG
OLTP_URL = "postgresql+psycopg2://oltp:ecommerce123@postgres_oltp:5432/ecommerce_oltp"
OLAP_URL = "postgresql+psycopg2://olap:ecommerce123@postgres_olap:5432/ecommerce_olap"
etl = ETLIncremental(oltp_url=OLTP_URL, olap_url=OLAP_URL)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'dag_incremental_load_olap',
    default_args=default_args,
    description='DAG para carga incremental robusta',
    schedule_interval='@hourly',
    start_date=pendulum.datetime(2025, 4, 25, 14, 0, tz="America/Sao_Paulo"),
    catchup=False,
)

# Funções das tarefas
def extract_task(**kwargs):
    last_execution_date = kwargs.get('prev_execution_date')
    if last_execution_date is None:
        last_execution_date = pendulum.datetime(2025, 4, 22, tz="America/Sao_Paulo")
    else:
        # Converte prev_execution_date para pendulum.DateTime e ajusta o fuso horário
        last_execution_date = pendulum.instance(last_execution_date).astimezone(pendulum.timezone("America/Sao_Paulo"))
    
    # Converte para UTC, já que o banco OLTP está em UTC
    last_execution_date_utc = last_execution_date.in_timezone('UTC')
    
    # Logs para depuração
    print(f"prev_execution_date (original): {kwargs.get('prev_execution_date')}")
    print(f"last_execution_date (America/Sao_Paulo): {last_execution_date}")
    print(f"last_execution_date (UTC): {last_execution_date_utc}")
    
    extracted_data = etl.extract(last_execution_date.to_datetime_string())
    kwargs['ti'].xcom_push(key='extracted_data', value=extracted_data)

def transform_dimensions_task(**kwargs):
    extracted_data = kwargs['ti'].xcom_pull(key='extracted_data', task_ids='extract_task')
    transformed_dimensions = etl.transform_dimensions(extracted_data)
    kwargs['ti'].xcom_push(key='transformed_dimensions', value=transformed_dimensions)

def load_dimensions_task(**kwargs):
    transformed_dimensions = kwargs['ti'].xcom_pull(key='transformed_dimensions', task_ids='transform_dimensions_task')
    etl.load_dimensions(transformed_dimensions)

def transform_facts_task(**kwargs):
    extracted_data = kwargs['ti'].xcom_pull(key='extracted_data', task_ids='extract_task')
    transformed_facts = etl.transform_facts(extracted_data)
    kwargs['ti'].xcom_push(key='transformed_facts', value=transformed_facts)

def load_facts_task(**kwargs):
    transformed_facts = kwargs['ti'].xcom_pull(key='transformed_facts', task_ids='transform_facts_task')
    etl.load_facts(transformed_facts)


# Definição das tarefas
extract_op = PythonOperator(
    task_id='extract_task',
    python_callable=extract_task,
    provide_context=True,
    dag=dag,
)

transform_dimensions_op = PythonOperator(
    task_id='transform_dimensions_task',
    python_callable=transform_dimensions_task,
    provide_context=True,
    dag=dag,
)

load_dimensions_op = PythonOperator(
    task_id='load_dimensions_task',
    python_callable=load_dimensions_task,
    provide_context=True,
    dag=dag,
)

transform_facts_op = PythonOperator(
    task_id='transform_facts_task',
    python_callable=transform_facts_task,
    provide_context=True,
    dag=dag,
)

load_facts_op = PythonOperator(
    task_id='load_facts_task',
    python_callable=load_facts_task,
    provide_context=True,
    dag=dag,
)

# Dependências
extract_op >> transform_dimensions_op >> load_dimensions_op >> transform_facts_op >> load_facts_op