from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, DECIMAL, TIMESTAMP, Date, CHAR, CheckConstraint, BIGINT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from os import path
from pandas import read_csv
# config new database Ecommerce_OLTP
DATABASE_URL = "postgresql+psycopg2://oltp:ecommerce123@postgres_oltp:5432/ecommerce_oltp" # Just because its a project doc
engine = create_engine(DATABASE_URL)
Base = declarative_base()

# Path to the CSV file
dag_dir = path.dirname(path.abspath(__file__))
csv_path = path.join(dag_dir, "utils", "items_and_categories.csv")

# Read the CSV using pandas
df_items_and_categories = read_csv(csv_path)

# Constraint for sizes
valid_sizes = set()
for sizes in df_items_and_categories["tamanhos_validos"]:
    for size in sizes.split(";"):
        valid_sizes.add(size)

sizes_constraint = ", ".join(f"'{size}'" for size in valid_sizes)
constraint_sizes_sql = f"size IN ({sizes_constraint})"

# Constraint for categories:
valid_categories = set(df_items_and_categories["categoria"])
categories_constraint = ", ".join(f"'{category}'" for category in valid_categories)
category_constraint_sql = f"name IN ({categories_constraint})"

class Category(Base):
    __tablename__ = "categories"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False)
    CheckConstraint(
            category_constraint_sql,
            name="check_category_values"
        ),

class Items(Base):
    __tablename__ = "items"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False)
    category_id = Column(Integer, ForeignKey("categories.id"), nullable=False)

class Size(Base):
    __tablename__ = "sizes"
    id = Column(Integer, primary_key=True, autoincrement=True)
    size = Column(String(10), nullable=False)
    CheckConstraint(
            constraint_sizes_sql,
            name="check_size_values"
        )


class Items_sizes(Base):
    __tablename__ = "items_sizes"
    item_id = Column(Integer,ForeignKey("items.id"),primary_key=True)
    size_id = Column(Integer,ForeignKey("sizes.id"),primary_key=True)

class Prices(Base):
    __tablename__ = "prices"
    item_id = Column(Integer, ForeignKey("items.id"), primary_key=True)
    size_id = Column(Integer, ForeignKey("sizes.id"), primary_key=True)
    value = Column(DECIMAL, nullable=False)
    __table_args__ = (CheckConstraint('value > 0', name='check_positive_price'),)

class Stores(Base):
    __tablename__ = "stores"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False)
    addresses = relationship("StoresAddresses", back_populates="store", cascade="all, delete-orphan", passive_deletes=True)
    phones = relationship("PhonesStores", back_populates="store", cascade="all, delete-orphan", passive_deletes=True)

class Addresses(Base):
    __tablename__ = "addresses"
    id = Column(Integer, primary_key=True, autoincrement=True)
    street = Column(String(100), nullable=False)
    number = Column(String(10), nullable=False)
    complement = Column(String(50))
    neighborhood = Column(String(50))
    city = Column(String(50))
    state = Column(CHAR(2), nullable=False)
    zip_code = Column(String(10), nullable=False)
    country = Column(String(50), default='Brasil')
    __table_args__ = (
        CheckConstraint("state IN ('AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO')", name="check_valid_state"),
        CheckConstraint("zip_code ~ '^[0-9]{5}(-?[0-9]{3})?$'", name="check_zip_code_format")
    )

class StoresAddresses(Base):
    __tablename__ = "stores_addresses"
    store_id = Column(Integer, ForeignKey("stores.id", ondelete="CASCADE"), primary_key=True)
    address_id = Column(Integer, ForeignKey("addresses.id"), primary_key=True)
    store = relationship("Stores", back_populates="addresses")

class Customers(Base):
    __tablename__ = "customers"
    customer_id = Column(Integer, primary_key=True, autoincrement=True)
    full_name = Column(String(50), nullable=False)
    email = Column(String(50), nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp())
    addresses = relationship("CustomersAddresses", back_populates="customer", cascade="all, delete-orphan", passive_deletes=True)
    phones = relationship("PhonesCustomers", back_populates="customer", cascade="all, delete-orphan", passive_deletes=True)

class CustomersAddresses(Base):
    __tablename__ = "customers_addresses"
    customer_id = Column(Integer, ForeignKey("customers.customer_id", ondelete="CASCADE"), primary_key=True)
    address_id = Column(Integer, ForeignKey("addresses.id"), primary_key=True)
    customer = relationship("Customers", back_populates="addresses")

class Phones(Base):
    __tablename__ = "phones"
    id = Column(Integer, primary_key=True, autoincrement=True)
    phone_type = Column(String(20))
    number = Column(String(20), nullable=False)
    # __table_args__ = (CheckConstraint("number ~ '^[0-9]+$'", name="check_phone_number_format"),)
    __table_args__ = (
        CheckConstraint(
            r"""number ~ '^(?:\(?(
                1[1-9]|2[1-4]|27|28|
                3[1-8]|4[1-6]|4[7-9]|
                5[1,3-5]|6[1-4]|6[5-7]|68|69|
                7[1,3-5,7,9]|
                8[1-3]|8[4-9]|
                9[1-4]|9[5-7]|98|99
            )\)?[-\s]?)(\d{4,5})[-\s]?(\d{4})$'""".replace("\n", "").replace(" ", ""),
            name="check_valid_brazilian_phone_number"
        ),

        CheckConstraint(
        "phone_type IN ('Residential', 'Mobile', 'Commercial')",
        name="check_valid_phone_type"
         )
        )

class PhonesCustomers(Base):
    __tablename__ = "phones_customers"
    phone_id = Column(Integer, ForeignKey("phones.id", ondelete="CASCADE"), primary_key=True)
    customer_id = Column(Integer, ForeignKey("customers.customer_id", ondelete="CASCADE"), primary_key=True)
    customer = relationship("Customers", back_populates="phones")

class PhonesStores(Base):
    __tablename__ = "phones_stores"
    phone_id = Column(Integer, ForeignKey("phones.id", ondelete="CASCADE"), primary_key=True)
    store_id = Column(Integer, ForeignKey("stores.id", ondelete="CASCADE"), primary_key=True)
    store = relationship("Stores", back_populates="phones")

class Purchases(Base):
    __tablename__ = "purchases"
    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(Integer, ForeignKey("customers.customer_id"), nullable=True)
    item_id = Column(Integer, ForeignKey("items.id"), nullable=False)
    size_id = Column(Integer, ForeignKey("sizes.id"), nullable=False)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    order_date = Column(Date, nullable=False)
    created_at = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp())
    status = relationship("PurchaseStatus", back_populates="purchase")

class PurchaseStatus(Base):
    __tablename__ = "purchases_status"
    purchase_id = Column(Integer, ForeignKey("purchases.id", ondelete="CASCADE"), primary_key=True, nullable=False)
    status = Column(String(20), nullable=False)
    purchase = relationship("Purchases", back_populates="status")
    __table_args__ = (CheckConstraint("status IN ('Pending', 'Sent', 'Delivered', 'Canceled')", name='check_valid_status'),)

class Inventory(Base):
    __tablename__ = "inventory"
    item_id = Column(Integer, ForeignKey("items.id"), primary_key=True, nullable=False)
    size_id = Column(Integer, ForeignKey("sizes.id"), primary_key=True, nullable=False)
    store_id = Column(Integer, ForeignKey("stores.id"), primary_key=True, nullable=False)
    quantity = Column(BIGINT, nullable=False)

    __table_args__ = (CheckConstraint('quantity >= 0', name='check_not_negative_quantity'),)

def create_tables():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    print("The OLTP tables have been created =D")

with DAG(
    dag_id='create_ecommerce_oltp_tables',
    start_date=datetime(2025, 4, 2),  
    schedule_interval="@once",  
    catchup=False,
    is_paused_upon_creation=False
) as dag:
    
    create_oltp_tables_task = PythonOperator(
        task_id='create_oltp_tables',
        python_callable=create_tables,
    )

