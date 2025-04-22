from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from faker import Faker
import random
from utils.phone_utils import generate_random_phone_number
from utils.customer_email import generate_customer_email
from utils.brazilian_address_complement import generate_brazilian_address_complement

# Database configuration
DATABASE_URL = "postgresql+psycopg2://oltp:ecommerce123@postgres_oltp:5432/ecommerce_oltp"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
MAX_PURCHASES = 100
fake = Faker('pt_BR')

# Configurable probabilities
P_NO_CUSTOMER = 0.3
P_EXISTING_CUSTOMER = 0.3
P_NEW_CUSTOMER = 0.4
P_NULL_COMPLEMENT = 0.3
P_NULL_PHONE = 0.2

class Customer:
    def __init__(self, session):
        self.session = session

    def create_new_customer(self):
        full_name = fake.name()
        customer_data = {
            "full_name": full_name,
            "email": generate_customer_email(full_name)
        }
        customer_result = self.session.execute(
            text("INSERT INTO customers (full_name, email) VALUES (:full_name, :email) RETURNING customer_id"),
            customer_data
        )
        customer_id = customer_result.fetchone()[0]

        address_data = {
            "street": fake.street_name(),
            "number": str(fake.building_number()),
            "complement": None if random.random() < P_NULL_COMPLEMENT else generate_brazilian_address_complement(),
            "neighborhood": fake.bairro(),
            "city": fake.city(),
            "state": fake.estado_sigla(),
            "zip_code": fake.postcode(),
            "country": "Brasil"
        }
        address_result = self.session.execute(
            text("INSERT INTO addresses (street, number, complement, neighborhood, city, state, zip_code, country) "
                 "VALUES (:street, :number, :complement, :neighborhood, :city, :state, :zip_code, :country) RETURNING id"),
            address_data
        )
        address_id = address_result.fetchone()[0]

        self.session.execute(
            text("INSERT INTO customers_addresses (customer_id, address_id) VALUES (:customer_id, :address_id)"),
            {"customer_id": customer_id, "address_id": address_id}
        )

        if random.random() >= P_NULL_PHONE:
            phone_dict = generate_random_phone_number()
            phone_result = self.session.execute(
                text("INSERT INTO phones (phone_type, number) VALUES (:phone_type, :number) RETURNING id"),
                phone_dict
            )
            phone_id = phone_result.fetchone()[0]

            self.session.execute(
                text("INSERT INTO phones_customers (phone_id, customer_id) VALUES (:phone_id, :customer_id)"),
                {"phone_id": phone_id, "customer_id": customer_id}
            )

        return customer_id

    def get_random_existing_customer(self):
        customer_row = self.session.execute(
            text("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1")
        ).fetchone()
        return customer_row[0] if customer_row else None

class Inventory:
    def __init__(self, session):
        self.session = session

    def check_stock(self, item_id, size_id, store_id, quantity=1):
        inventory_row = self.session.execute(
            text("""
                SELECT quantity FROM inventory 
                WHERE item_id = :item_id 
                  AND size_id = :size_id 
                  AND store_id = :store_id
            """),
            {"item_id": item_id, "size_id": size_id, "store_id": store_id}
        ).fetchone()
        return inventory_row and inventory_row[0] >= quantity

    def decrement_stock(self, item_id, size_id, store_id, quantity=1):
        self.session.execute(
            text("""
                UPDATE inventory 
                SET quantity = quantity - :quantity 
                WHERE item_id = :item_id 
                  AND size_id = :size_id 
                  AND store_id = :store_id
            """),
            {"item_id": item_id, "size_id": size_id, "store_id": store_id, "quantity": quantity}
        )

class Purchase:
    def __init__(self, session):
        self.session = session

    def create_purchase(self, customer_id, store_id, items_sizes_quantities):
        order_date = fake.date_between(start_date=date(2020, 10, 31), end_date=date.today())
        purchase_data = {
            "customer_id": customer_id,
            "store_id": store_id,
            "order_date": order_date
        }
        result = self.session.execute(
            text("""
                INSERT INTO purchases (customer_id, store_id, order_date) 
                VALUES (:customer_id, :store_id, :order_date)
                RETURNING id
            """),
            purchase_data
        )
        purchase_id = result.fetchone()[0]

        for item in items_sizes_quantities:
            item_data = {
                "purchase_id": purchase_id,
                "item_id": item["item_id"],
                "size_id": item["size_id"],
                "quantity": item["quantity"]
            }
            self.session.execute(
                text("""
                    INSERT INTO purchases_items (purchase_id, item_id, size_id, quantity) 
                    VALUES (:purchase_id, :item_id, :size_id, :quantity)
                """),
                item_data
            )

        purchase_status_data = {
            "purchase_id": purchase_id,
            "status": random.choice(["Pending", "Sent", "Delivered", "Canceled"])
        }
        self.session.execute(
            text("""
                INSERT INTO purchases_status (purchase_id, status) 
                VALUES (:purchase_id, :status)
            """),
            purchase_status_data
        )
        return purchase_id

class EcommerceManager:
    def __init__(self, session):
        self.session = session
        self.customer = Customer(session)
        self.inventory = Inventory(session)
        self.purchase = Purchase(session)

    def register_purchases_and_customers(self):
        purchase_count = self.session.execute(text("SELECT COUNT(*) FROM purchases")).scalar()
        customer_count = self.session.execute(text("SELECT COUNT(*) FROM customers")).scalar()

        if purchase_count >= MAX_PURCHASES:
            print("Limit reached. No purchases or customers will be registered.")
            return

        num_purchases = random.randint(10, 50)
        for _ in range(num_purchases):
            store_row = self.session.execute(
                text("SELECT id FROM stores ORDER BY RANDOM() LIMIT 1")
            ).fetchone()
            if not store_row:
                print("No stores found in the stores table.")
                continue
            store_id = store_row[0]

            roll = random.random()
            if roll < P_NO_CUSTOMER and customer_count > 0:
                customer_id = None
            elif roll < (P_NO_CUSTOMER + P_EXISTING_CUSTOMER) and customer_count > 0:
                customer_id = self.customer.get_random_existing_customer()
            else:
                customer_id = self.customer.create_new_customer()

            num_items = random.randint(1, 5)
            items_sizes_quantities = []
            combination_to_index = {}

            for _ in range(num_items):
                item_size_row = self.session.execute(
                    text("SELECT item_id, size_id FROM items_sizes ORDER BY RANDOM() LIMIT 1")
                ).fetchone()
                if not item_size_row:
                    print("No item/size combination found.")
                    continue
                item_id, size_id = item_size_row
                combination = (item_id, size_id)

                if combination in combination_to_index:
                    idx = combination_to_index[combination]
                    items_sizes_quantities[idx]["quantity"] += random.randint(1, 3)
                else:
                    quantity = random.randint(1, 3)
                    items_sizes_quantities.append({
                        "item_id": item_id,
                        "size_id": size_id,
                        "quantity": quantity
                    })
                    combination_to_index[combination] = len(items_sizes_quantities) - 1

            valid_items = []
            for item in items_sizes_quantities:
                item_id = item["item_id"]
                size_id = item["size_id"]
                quantity = item["quantity"]
                if self.inventory.check_stock(item_id, size_id, store_id, quantity):
                    valid_items.append(item)
                else:
                    print(f"Insufficient stock for item {item_id}, size {size_id}, store {store_id}, quantity {quantity}. Item skipped.")

            if valid_items:
                purchase_id = self.purchase.create_purchase(customer_id, store_id, valid_items)
                for item in valid_items:
                    self.inventory.decrement_stock(item["item_id"], item["size_id"], store_id, item["quantity"])
            else:
                print(f"No items available with sufficient stock for the purchase in store {store_id}.")

        self.session.commit()
        print(f"{num_purchases} purchase attempt(s) processed.")

# Configuração do DAG
with DAG(
    dag_id='register_purchases_and_clients',
    start_date=datetime(2025, 4, 2),
    schedule_interval=timedelta(minutes=1),
    catchup=False,
) as dag:
    def run_ecommerce_manager():
        session = Session()
        manager = EcommerceManager(session)
        manager.register_purchases_and_customers()
        session.close()

    register_task = PythonOperator(
        task_id='register_purchases_and_customers',
        python_callable=run_ecommerce_manager,
    )