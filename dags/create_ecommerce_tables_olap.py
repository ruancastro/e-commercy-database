from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Float, TIMESTAMP, Date, CHAR, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

DATABASE_URL = "postgresql+psycopg2://ecommerce_oltp:ecommerce123@postgres:5432/Ecommerce_OLTP"
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
    
class FactInventory(Base):
    __tablename__ = "fact_inventory"
    item_id = Column(Integer, ForeignKey('dim_items.item_id'), nullable=False)
    size_id = Column(Integer, ForeignKey('dim_sizes.size_id'), nullable=False)
    store_id = Column(Integer, ForeignKey('dim_stores.store_id'), nullable=False)
    date_id = Column(Integer, ForeignKey('dim_time.date_id'), nullable=False)
    quantity_in_stock = Column(Integer, nullable=False)
    __table_args__ = (PrimaryKeyConstraint('item_id', 'size_id', 'store_id', 'date_id'),)

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

Base.metadata.create_all(engine)