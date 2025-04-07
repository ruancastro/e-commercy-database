from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Float, TIMESTAMP, Date, CHAR, PrimaryKeyConstraint, CheckConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

DATABASE_URL = "postgresql+psycopg2://olap:ecommerce123@postgres:5432/Ecommerce_OLAP"
engine = create_engine(DATABASE_URL)
Base = declarative_base()

class FactSales(Base):
    __tablename__ = "fact_sales"
    purchase_id = Column(Integer, primary_key=True, nullable=False)
    customer_id = Column(Integer, ForeignKey('dim_customers.customer_id'), nullable=True)
    item_id = Column(Integer, ForeignKey('dim_items.item_id'), nullable=False)
    size_id = Column(Integer, ForeignKey('dim_sizes.size_id'), nullable=False)
    store_id = Column(Integer, ForeignKey('dim_stores.store_id'), nullable=False)
    date_id = Column(Integer, ForeignKey('dim_time.date_id'), nullable=False)
    total_value = Column(Float, nullable=False)
    quantity_sold = Column(Integer, nullable=False)
    customer = relationship("DimCustomers", back_populates="sales")
    __table_args__ = (
        CheckConstraint('total_value >= 0', name='check_total_value_not_negative'),
        CheckConstraint('quantity_sold >= 0', name='check_quantity_sold_not_negative'),
    )

class FactInventory(Base):
    __tablename__ = "fact_inventory"
    item_id = Column(Integer, ForeignKey('dim_items.item_id'), nullable=False)
    size_id = Column(Integer, ForeignKey('dim_sizes.size_id'), nullable=False)
    store_id = Column(Integer, ForeignKey('dim_stores.store_id'), nullable=False)
    date_id = Column(Integer, ForeignKey('dim_time.date_id'), nullable=False)
    quantity_in_stock = Column(Integer, nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint('item_id', 'size_id', 'store_id', 'date_id'),
        CheckConstraint('quantity_in_stock >= 0', name='quantity_in_stock_not_negative'),
    )

class DimTime(Base):
    __tablename__ = "dim_time"
    date_id = Column(Integer, primary_key=True, nullable=False)
    date = Column(Date, nullable=False)
    day = Column(Integer, nullable=False) 
    month = Column(Integer, nullable=False) 
    quarter = Column(Integer, nullable=False) 
    year = Column(Integer, nullable=False) 

class DimSizes(Base):
    __tablename__ = "dim_sizes"
    size_id = Column(Integer, primary_key=True, nullable=False)
    size = Column(String(4), nullable=False)

class DimStores(Base):
    __tablename__ = "dim_stores"
    store_id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(50), nullable=False)
    city = Column(String(50))
    state = Column(CHAR(2), nullable=False)
    zip_code = Column(String(10), nullable=False)
    __table_args__ = (
        CheckConstraint("state IN ('AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO')", name="check_valid_state"),
        CheckConstraint("zip_code ~ '^[0-9]{5}(-?[0-9]{3})?$'", name="check_zip_code_format")
    )

class DimItems(Base):
    __tablename__ = "dim_items"
    item_id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(50), nullable=True)
    category_name = Column(String(50), nullable=False)

class DimCustomers(Base):
    __tablename__ = "dim_customers"
    customer_id = Column(Integer, primary_key=True, nullable=False)
    full_name = Column(String(50), nullable=False)
    email = Column(String(50), nullable=False)
    created_at = Column(TIMESTAMP, nullable=False)
    sales = relationship("FactSales", back_populates="customer")

def create_tables():
    Base.metadata.create_all(engine)
    print("The OLAP tables have been created =D")


with DAG (
    dag_id = 'create_ecommerce_olap_tables',
    start_date = datetime(2025,4,7),
    schedule_interval = "@once",
    catchup = False
) as dag:
    create_olap_tables_task = PythonOperator(
        task_id = "create_olap_tables",
        python_callable = create_tables,
    )