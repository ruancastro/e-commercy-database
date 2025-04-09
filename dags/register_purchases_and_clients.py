from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from faker import Faker
import random
from datetime import date
# Configuração do banco de dados
DATABASE_URL = "postgresql+psycopg2://oltp:ecommerce123@postgres_oltp:5432/ecommerce_oltp"  # Just because its a project doc
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
    """
    Registers purchases and, if necessary, new customers in the e-commerce database.

    This function performs the following steps:
      1. Checks the current count of purchases and customers. If either reaches 300, no new registrations occur.
      2. For a random number of purchase attempts (between 5 and 10):
         - Randomly selects an item and its size from the `prices` table.
         - Randomly selects a store from the `stores` table.
         - Determines whether the purchase will be associated with:
             a. No customer (customer_id is NULL),
             b. An existing customer, or
             c. A new customer (in which case the customer's address and, if applicable, phone are registered).
      3. Before registering a purchase, checks if the combination of (item_id, size_id, store_id) exists in the inventory with a quantity > 0.
         - If available, decrements the inventory quantity by 1 and registers the purchase in the `purchases` table.
         - Otherwise, logs an "out of stock" message indicating that the combination is unavailable, and the purchase is not recorded.
      4. Commits all changes to the database.

    Returns:
        None

    Side Effects:
        - Inserts rows into the `customers`, `addresses`, `customers_addresses`, `phones`, `phones_customers`, and `purchases` tables.
        - Updates the `inventory` table by reducing the quantity for the matching (item_id, size_id, store_id) combination.
        - Prints log messages indicating out-of-stock scenarios and the number of processed purchase attempts.
    """

    session = Session()

    # Check current purchase and customer counts
    purchase_count = session.execute(text("SELECT COUNT(*) FROM purchases")).scalar()
    customer_count = session.execute(text("SELECT COUNT(*) FROM customers")).scalar()

    # If 300 purchases or 300 customers are reached, do nothing
    if purchase_count >= 300 or customer_count >= 300:
        print("Limit reached. No purchase or customer will be registered.")
        session.close()
        return

    # Register between 5 and 10 purchases per execution
    num_purchases = random.randint(5, 10)
    for _ in range(num_purchases):
        # Randomly select an item and size combination from prices table
        price_row = session.execute(
            text("SELECT item_id, size_id FROM prices ORDER BY RANDOM() LIMIT 1")
        ).fetchone()
        item_id, size_id = price_row

        # Randomly select a store from the stores table
        store_row = session.execute(
            text("SELECT id FROM stores ORDER BY RANDOM() LIMIT 1")
        ).fetchone()
        store_id = store_row[0]

        # Decide if this purchase will have:
        # 1) No customer
        # 2) Existing customer
        # 3) New customer
        roll = random.random()

        if roll < P_NO_CUSTOMER and customer_count > 0:
            # 1) Purchase without customer
            customer_id = None

        elif roll < (P_NO_CUSTOMER + P_EXISTING_CUSTOMER) and customer_count > 0:
            # 2) Existing customer
            customer_row = session.execute(
                text("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1")
            ).fetchone()
            customer_id = customer_row[0]

        else:
            # 3) New customer (or if there are no customers yet)
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

            # Create address for the new customer in the addresses table
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

            # Register the customer's phone in the phones table (if applicable)
            phone_number = None if random.random() < P_NULL_PHONE else fake.numerify(text="###########")
            if phone_number:
                phone_data = {
                    "phone_type": "Residential",
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

        # Check if inventory is available for the selected combination
        inventory_row = session.execute(
            text("""
                SELECT quantity FROM inventory 
                WHERE item_id = :item_id 
                  AND size_id = :size_id 
                  AND store_id = :store_id
            """),
            {"item_id": item_id, "size_id": size_id, "store_id": store_id}
        ).fetchone()

        if inventory_row and inventory_row[0] > 0:
            # Decrement inventory quantity by 1
            session.execute(
                text("""
                    UPDATE inventory 
                    SET quantity = quantity - 1 
                    WHERE item_id = :item_id 
                      AND size_id = :size_id 
                      AND store_id = :store_id
                """),
                {"item_id": item_id, "size_id": size_id, "store_id": store_id}
            )

            # Register the purchase in the purchases table (customer_id can be NULL)
            order_date = fake.date_between(start_date=date(2020, 10, 31), end_date=date.today())
            purchase_data = {
                "customer_id": customer_id,
                "item_id": item_id,
                "size_id": size_id,
                "store_id": store_id,
                "order_date": order_date
            }

            result = session.execute(
                text("""
                    INSERT INTO purchases (customer_id, item_id, size_id, store_id, order_date) 
                    VALUES (:customer_id, :item_id, :size_id, :store_id, :order_date)
                    RETURNING id
                """),
                purchase_data
            )

            purchase_id = result.fetchone()[0]
            
            purchase_status_data = {
                'purchase_id': purchase_id,
                'status': random.choice(['Pending', 'Sent', 'Delivered', 'Canceled'])
            }

            session.execute(
                text("""
                    INSERT INTO purchases_status (purchase_id, status) 
                    VALUES (:purchase_id, :status)
                """),
                purchase_status_data
            )
        else:
            # If the combination is not available in inventory, log out of stock message
            print(f"Combination out of stock: item {item_id}, size {size_id}, store {store_id}. Purchase not registered.")

    session.commit()
    session.close()
    print(f"{num_purchases} purchase attempt(s) processed.")

with DAG(
    dag_id='register_purchases_and_clients',
    start_date=datetime(2025, 4, 2),
    schedule_interval=timedelta(minutes=2),
    catchup=False,
) as dag:
    register_task = PythonOperator(
        task_id='register_purchases_and_customers',
        python_callable=register_purchases_and_customers,
    )
