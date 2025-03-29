from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Float, TIMESTAMP, Date, CHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
import os 
# postgresql_psw env var

# Configuração do banco de dados
credential = {'user': 'postgres',
               'password': os.environ.get('postgresql_psw')}
DATABASE_URL = f"postgresql+psycopg2://{credential['user']}:{credential['password']}@localhost:5432/Ecommerce_OLTP"
engine = create_engine(DATABASE_URL)
Base = declarative_base()

# Definição das tabelas
class Category(Base):
    __tablename__ = "categories"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False)

class Item(Base):
    __tablename__ = "item"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False)
    category_id = Column(Integer, ForeignKey("categories.id"), nullable=False)

class Size(Base):
    __tablename__ = "sizes"
    id = Column(Integer, primary_key=True, autoincrement=True)
    size = Column(String(4), nullable=False)

class Price(Base):
    __tablename__ = "price"
    item_id = Column(Integer, ForeignKey("item.id"), primary_key=True)
    size_id = Column(Integer, ForeignKey("sizes.id"), primary_key=True)
    value = Column(Float, nullable=False)

class Store(Base):
    __tablename__ = "stores"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False)

class Address(Base):
    __tablename__ = "addresses"
    id = Column(Integer, primary_key=True, autoincrement=True)
    street = Column(String(100), nullable=False)
    number = Column(String(10))
    complement = Column(String(50))
    neighborhood = Column(String(50))
    city = Column(String(50), nullable=False)
    state = Column(CHAR(2), nullable=False)
    zip_code = Column(String(10), nullable=False)
    country = Column(String(50), default='Brasil')

class StoreAddress(Base):
    __tablename__ = "store_address"
    store_id = Column(Integer, ForeignKey("stores.id"), primary_key=True)
    address_id = Column(Integer, ForeignKey("addresses.id"), primary_key=True)

class Customer(Base):
    __tablename__ = "customers"
    customer_id = Column(Integer, primary_key=True, autoincrement=True)
    full_name = Column(String(50), nullable=False)
    email = Column(String(50), nullable=False)
    created_at = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP")

class CustomerAddress(Base):
    __tablename__ = "customer_addresses"
    customer_id = Column(Integer, ForeignKey("customers.customer_id"), primary_key=True)
    address_id = Column(Integer, ForeignKey("addresses.id"), primary_key=True)

class Phone(Base):
    __tablename__ = "phones"
    id = Column(Integer, primary_key=True, autoincrement=True)
    phone_type = Column(String(20))
    number = Column(String(20), nullable=False)

class PhoneCustomer(Base):
    __tablename__ = "phones_customers"
    phone_id = Column(Integer, ForeignKey("phones.id"), primary_key=True)
    customer_id = Column(Integer, ForeignKey("customers.customer_id"), primary_key=True)

class PhoneStore(Base):
    __tablename__ = "phones_store"
    phone_id = Column(Integer, ForeignKey("phones.id"), primary_key=True)
    store_id = Column(Integer, ForeignKey("stores.id"), primary_key=True)

class Purchase(Base):
    __tablename__ = "purchase"
    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(Integer, ForeignKey("customers.customer_id"), nullable=False)
    item_id = Column(Integer, nullable=False)
    size_id = Column(Integer, nullable=False)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    order_date = Column(Date, nullable=False)
    created_at = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP")

# Criando as tabelas no banco de dados
Base.metadata.create_all(engine)

print("Tabelas criadas com sucesso!")
