from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from faker import Faker
import random

# Configuração do banco de dados
DATABASE_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/Ecommerce_OLTP"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

fake = Faker('pt_BR')

# Probabilidades configuráveis:
P_NO_CUSTOMER = 0.3       # Chance de não haver cliente (purchase com customer_id = NULL)
P_EXISTING_CUSTOMER = 0.3 # Chance de usar cliente existente
P_NEW_CUSTOMER = 0.4      # Chance de criar novo cliente
# Probabilidades de campos nulos
P_NULL_COMPLEMENT = 0.3   # Chance do campo 'complement' ser NULL
P_NULL_PHONE = 0.2        # Chance do telefone ser NULL

def get_secondary_address():
    """Retorna secondary_address se disponível, senão usa street_suffix como alternativa."""
    if hasattr(fake, 'secondary_address'):
        return fake.secondary_address()
    else:
        return fake.street_suffix()

def register_purchases_and_customers():
    session = Session()

    # Verifica a contagem atual de compras e clientes
    purchase_count = session.execute(text("SELECT COUNT(*) FROM purchases")).scalar()
    customer_count = session.execute(text("SELECT COUNT(*) FROM customers")).scalar()

    # Se atingiu 300 compras ou 300 clientes, não faz mais nada
    if purchase_count >= 300 or customer_count >= 300:
        print("Limite atingido. Nenhuma compra ou cliente será registrado.")
        session.close()
        return

    # Registra de 5 a 10 compras por execução
    num_purchases = random.randint(5, 10)
    for _ in range(num_purchases):
        # Seleciona aleatoriamente uma combinação de item e tamanho da tabela prices
        price_row = session.execute(
            text("SELECT item_id, size_id FROM prices ORDER BY RANDOM() LIMIT 1")
        ).fetchone()
        item_id, size_id = price_row

        # Seleciona uma loja aleatória da tabela stores
        store_row = session.execute(
            text("SELECT id FROM stores ORDER BY RANDOM() LIMIT 1")
        ).fetchone()
        store_id = store_row[0]

        # Decide se esta compra terá:
        # 1) Nenhum cliente
        # 2) Cliente existente
        # 3) Novo cliente
        roll = random.random()

        if roll < P_NO_CUSTOMER and customer_count > 0:
            # 1) Compra sem cliente
            customer_id = None

        elif roll < (P_NO_CUSTOMER + P_EXISTING_CUSTOMER) and customer_count > 0:
            # 2) Cliente existente
            customer_row = session.execute(
                text("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1")
            ).fetchone()
            customer_id = customer_row[0]

        else:
            # 3) Novo cliente (ou se não há clientes cadastrados ainda)
            customer_data = {
                "full_name": fake.name(),
                "email": fake.email()
            }
            customer_result = session.execute(
                text("INSERT INTO customers (full_name, email) VALUES (:full_name, :email) RETURNING customer_id"),
                customer_data
            )
            customer_id = customer_result.fetchone()[0]
            customer_count += 1

            # Cria endereço para o novo cliente na tabela addresses
            address_data = {
                "street": fake.street_name(),
                "number": str(fake.building_number()),
                "complement": None if random.random() < P_NULL_COMPLEMENT else get_secondary_address(),
                "neighborhood": fake.bairro(),
                "city": fake.city(),
                "state": fake.estado_sigla(),
                "zip_code": fake.postcode(),
                "country": "Brasil"
            }
            address_result = session.execute(
                text("INSERT INTO addresses (street, number, complement, neighborhood, city, state, zip_code, country) "
                     "VALUES (:street, :number, :complement, :neighborhood, :city, :state, :zip_code, :country) RETURNING id"),
                address_data
            )
            address_id = address_result.fetchone()[0]

            session.execute(
                text("INSERT INTO customers_addresses (customer_id, address_id) VALUES (:customer_id, :address_id)"),
                {"customer_id": customer_id, "address_id": address_id}
            )

            # Registro do telefone do cliente na tabela phones (se aplicável)
            phone_number = None if random.random() < P_NULL_PHONE else fake.numerify(text="###########")
            if phone_number:
                phone_data = {
                    "phone_type": "Residencial",
                    "number": phone_number
                }
                phone_result = session.execute(
                    text("INSERT INTO phones (phone_type, number) VALUES (:phone_type, :number) RETURNING id"),
                    phone_data
                )
                phone_id = phone_result.fetchone()[0]

                session.execute(
                    text("INSERT INTO phones_customers (phone_id, customer_id) VALUES (:phone_id, :customer_id)"),
                    {"phone_id": phone_id, "customer_id": customer_id}
                )

        # Registrar a compra na tabela purchase (customer_id pode ser NULL)
        purchase_data = {
            "customer_id": customer_id,
            "item_id": item_id,
            "size_id": size_id,
            "store_id": store_id,
            "order_date": datetime.now().date()
        }
        session.execute(
            text("INSERT INTO purchases (customer_id, item_id, size_id, store_id, order_date) "
                 "VALUES (:customer_id, :item_id, :size_id, :store_id, :order_date)"),
            purchase_data
        )

    session.commit()
    session.close()
    print(f"{num_purchases} purchases registered.")

# Definição da DAG para execução a cada 5 minutos
with DAG(
    dag_id='register_purchases_and_clients',
    start_date=datetime(2025, 4, 2),
    schedule_interval=timedelta(minutes=3),
    catchup=False,
) as dag:
    register_task = PythonOperator(
        task_id='register_purchases_and_customers',
        python_callable=register_purchases_and_customers,
    )
