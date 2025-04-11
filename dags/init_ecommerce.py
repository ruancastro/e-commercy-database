from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.ecommerce_starter import EcommerceStarter
from os import path

DATABASE_URL = "postgresql+psycopg2://oltp:ecommerce123@postgres_oltp:5432/ecommerce_oltp"  # Just because its a project doc

STORES_QUANTITY = 10
ITEMS_QUANTITY = 40  # MAX 100

dag_dir = path.dirname(path.abspath(__file__))
path_root_csv = path.join(dag_dir, "utils", "items_and_categories.csv")

ecommerce_starter = EcommerceStarter(
    DATABASE_URL=DATABASE_URL,
    path_root_csv=path_root_csv,
    stores_quantity=STORES_QUANTITY,
    items_quantity=ITEMS_QUANTITY,
)

# Definição da DAG para inicialização dos dados
with DAG(
    dag_id="init_ecommerce",
    start_date=datetime(2025, 4, 2),
    schedule_interval="@once",
    catchup=False,
) as dag:
    init_task = PythonOperator(
        task_id="create_initial_data",
        python_callable=ecommerce_starter.create_initial_data,
    )
