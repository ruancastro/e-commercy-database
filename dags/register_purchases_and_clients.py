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
MAX_PURCHASES = 500
fake = Faker('pt_BR')

# Configurable probabilities
P_NO_CUSTOMER = 0.3       # Chance of no customer (purchase with customer_id = NULL)
P_EXISTING_CUSTOMER = 0.3 # Chance of using an existing customer
P_NEW_CUSTOMER = 0.4      # Chance of creating a new customer
P_NULL_COMPLEMENT = 0.3   # Chance of the 'complement' field being NULL
P_NULL_PHONE = 0.2        # Chance of the phone being NULL

class Customer:
    """
    Class responsible for operations related to customers, such as creating new customers with addresses and phones.
    """
    
    def __init__(self, session):
        self.session = session

    def create_new_customer(self):
        """
        Creates a new customer, including their address and phone (if applicable).

        Returns:
            int: The ID of the newly created customer.
        """
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

        # Create an address for the new customer
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

        # Register the customer's phone (if applicable)
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
        """
        Retrieves the ID of a random existing customer from the database.

        Returns:
            int or None: The ID of a random customer, or None if no customers exist.
        """
        customer_row = self.session.execute(
            text("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1")
        ).fetchone()
        return customer_row[0] if customer_row else None

class Inventory:
    """
    Class responsible for inventory-related operations, such as checking and updating stock quantities.
    """
    
    def __init__(self, session):
        self.session = session

    def check_stock(self, item_id, size_id, store_id):
        """
        Checks if the specific combination of item, size, and store is in stock.

        Args:
            item_id (int): The ID of the item.
            size_id (int): The ID of the size.
            store_id (int): The ID of the store.

        Returns:
            bool: True if the quantity is greater than 0, False otherwise.
        """
        inventory_row = self.session.execute(
            text("""
                SELECT quantity FROM inventory 
                WHERE item_id = :item_id 
                  AND size_id = :size_id 
                  AND store_id = :store_id
            """),
            {"item_id": item_id, "size_id": size_id, "store_id": store_id}
        ).fetchone()
        return inventory_row and inventory_row[0] > 0

    def decrement_stock(self, item_id, size_id, store_id):
        """
        Decrements the stock quantity by 1 for the specific combination of item, size, and store.

        Args:
            item_id (int): The ID of the item.
            size_id (int): The ID of the size.
            store_id (int): The ID of the store.
        """
        self.session.execute(
            text("""
                UPDATE inventory 
                SET quantity = quantity - 1 
                WHERE item_id = :item_id 
                  AND size_id = :size_id 
                  AND store_id = :store_id
            """),
            {"item_id": item_id, "size_id": size_id, "store_id": store_id}
        )

class Purchase:
    """
    Class responsible for operations related to purchases, such as creating new purchases and associating them with customers.
    """
    
    def __init__(self, session):
        self.session = session

    def create_purchase(self, customer_id, item_id, size_id, store_id):
        """
        Creates a new purchase and associates it with the specified customer, item, size, and store.

        Args:
            customer_id (int or None): The ID of the customer, or None if no customer is associated.
            item_id (int): The ID of the item.
            size_id (int): The ID of the size.
            store_id (int): The ID of the store.

        Returns:
            int: The ID of the newly created purchase.
        """
        order_date = fake.date_between(start_date=date(2020, 10, 31), end_date=date.today())
        purchase_data = {
            "customer_id": customer_id,
            "item_id": item_id,
            "size_id": size_id,
            "store_id": store_id,
            "order_date": order_date
        }
        result = self.session.execute(
            text("""
                INSERT INTO purchases (customer_id, item_id, size_id, store_id, order_date) 
                VALUES (:customer_id, :item_id, :size_id, :store_id, :order_date)
                RETURNING id
            """),
            purchase_data
        )
        purchase_id = result.fetchone()[0]

        # Insert the purchase status
        purchase_status_data = {
            'purchase_id': purchase_id,
            'status': random.choice(['Pending', 'Sent', 'Delivered', 'Canceled'])
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
    """
    Class that coordinates the registration of purchases and customers in the e-commerce database.
    """
    
    def __init__(self, session):
        self.session = session
        self.customer = Customer(session)
        self.inventory = Inventory(session)
        self.purchase = Purchase(session)

    def register_purchases_and_customers(self):
        """
        Registers purchases and, if necessary, new customers in the e-commerce database.

        Steps:
          1. Checks the current count of purchases. If MAX_PURCHASES is reached, no registrations occur.
          2. For a random number of purchase attempts (between 1 and 25):
             - Randomly selects an item and size combination from the `items_sizes` table.
             - Randomly selects a store from the `stores` table.
             - Decides whether the purchase will be associated with:
                 a. No customer (customer_id NULL),
                 b. An existing customer, or
                 c. A new customer (creating their address and phone if applicable).
          3. Before registering a purchase, checks if the (item_id, size_id, store_id) combination is in stock.
             - If available, decrements the stock and registers the purchase.
             - Otherwise, logs an "out of stock" message.
          4. Commits all changes to the database.
        """
        # Check the current purchase count
        purchase_count = self.session.execute(text("SELECT COUNT(*) FROM purchases")).scalar()
        customer_count = self.session.execute(text("SELECT COUNT(*) FROM customers")).scalar()

        if purchase_count >= MAX_PURCHASES:
            print("Limit reached. No purchases or customers will be registered.")
            return

        # Register between 5 and 10 purchases per execution
        num_purchases = random.randint(1, 25)
        for _ in range(num_purchases):
            # Randomly select an item and size combination from the items_sizes table
            item_size_row = self.session.execute(
                text("SELECT item_id, size_id FROM items_sizes ORDER BY RANDOM() LIMIT 1")
            ).fetchone()
            if not item_size_row:
                print("No valid item-size combinations found in items_sizes table.")
                continue
            item_id, size_id = item_size_row

            # Randomly select a store from the stores table
            store_row = self.session.execute(
                text("SELECT id FROM stores ORDER BY RANDOM() LIMIT 1")
            ).fetchone()
            if not store_row:
                print("No stores found in the stores table.")
                continue
            store_id = store_row[0]

            # Decide if the purchase will have:
            # 1) No customer
            # 2) Existing customer
            # 3) New customer
            roll = random.random()

            if roll < P_NO_CUSTOMER and customer_count > 0:
                customer_id = None
            elif roll < (P_NO_CUSTOMER + P_EXISTING_CUSTOMER) and customer_count > 0:
                customer_id = self.customer.get_random_existing_customer()
            else:
                customer_id = self.customer.create_new_customer()

            # Check if the selected combination is available in stock
            if self.inventory.check_stock(item_id, size_id, store_id):
                self.inventory.decrement_stock(item_id, size_id, store_id)
                self.purchase.create_purchase(customer_id, item_id, size_id, store_id)
            else:
                print(f"Combination out of stock: item {item_id}, size {size_id}, store {store_id}. Purchase not registered.")

        self.session.commit()
        print(f"{num_purchases} purchase attempt(s) processed.")

# DAG configuration
with DAG(
    dag_id='register_purchases_and_clients',
    start_date=datetime(2025, 4, 2),
    schedule_interval=timedelta(minutes=2),
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