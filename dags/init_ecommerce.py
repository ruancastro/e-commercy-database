from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from faker import Faker
import random
from utils.phone_utils import generate_random_phone_number
from pandas import read_csv
from os import path

DATABASE_URL = "postgresql+psycopg2://oltp:ecommerce123@postgres_oltp:5432/ecommerce_oltp"  # Just because its a project doc
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

fake = Faker('pt_BR')
STORES_QUANTITY = 10
ITEMS_QUANTITY = 60 #MAX 100

dag_dir = path.dirname(path.abspath(__file__))
csv_path = path.join(dag_dir, "utils", "items_and_categories.csv")
df_items_and_categories = read_csv(csv_path)

categories = df_items_and_categories['categoria'].unique()

items = df_items_and_categories['nome_item'].unique()
items_used = items[0:ITEMS_QUANTITY]

stores_names = [
    "Estilo Total",
    "Casa Bela",
    "Mundo dos Livros",
    "Tech Mania",
    "Esporte em Alta",
    "Conecta Shop",
    "Leitura Certa",
    "Decor Mix",
    "Moda Urbana",
    "Esporte Livre",
    "Gadgets Pro",
    "Universo Fashion",
    "Casa e Conforto",
    "Estacao Eletronica",
    "Toque Final",
    "Leve e Leia",
    "Top Fit",
    "Loja do Futuro",
    "Trend Store",
    "Viva Estilo"
]

stores_names_used = stores_names[0:STORES_QUANTITY]

# Lista de complementos de endereço comuns no Brasil
address_complements = [
    "Apto 101", "Casa", "Bloco A", "Sala 202", "Andar 3", "Ap 401", None, 
    "Bloco B", "Cobertura", "Térreo", "Andar 5", "Sala 305", "Ap 502", None, 
    "Bloco C", "Loja 10", "Andar 7", "Sala 102", "Apto 203", None, 
    "Galpão 1", "Andar 2", "Bloco D", "Sala 408", "Ap 601", None, 
    "Conjunto 15", "Torre 1", "Andar 9", "Sala 207", "Apto 705", None, 
    "Bloco E", "Sobreloja", "Sala 501", "Andar 4", "Apto 801", None, 
    "Torre 2", "Sala 303", "Andar 6", "Ap 904", "Bloco F", None, 
    "Piso Superior", "Loja 5", "Andar 10", "Sala 605", "Apto 1002", None
]

def create_initial_data():
    session = Session()

    # --- População de Categorias (tabela "categories") ---
    category_ids = {}
    for category in categories:
        
        category = {'name': category}
        result = session.execute(
            text("INSERT INTO categories (name) VALUES (:name) RETURNING id"),
            category
        )
        category_ids[category["name"]] = result.scalar()


    # --- População de Tamanhos (tabela "sizes") ---
    size_ids = []
    for size in ['P', 'M', 'G', 'GG']:
        result = session.execute(
            text("INSERT INTO sizes (size) VALUES (:size) RETURNING id"),
            {"size": size}
        )
        size_ids.append(result.fetchone()[0])

    # --- População de Endereços para Lojas (tabela "addresses") ---
    address_ids = []
    for _ in range(STORES_QUANTITY):  # NUMBER_OF_STORES endereços, um por loja
        address = {
            "street": fake.street_name(),
            "number": str(fake.building_number()),
            "complement": random.choice(address_complements) if random.random() >= 0.3 else None,
            "neighborhood": fake.bairro(),
            "city": fake.city(),
            "state": fake.estado_sigla(),
            "zip_code": fake.postcode(),
            "country": "Brasil"
        }
        result = session.execute(
            text("INSERT INTO addresses (street, number, complement, neighborhood, city, state, zip_code, country) "
                 "VALUES (:street, :number, :complement, :neighborhood, :city, :state, :zip_code, :country) RETURNING id"),
            address
        )
        address_ids.append(result.fetchone()[0])

    # --- População de Telefones para Lojas (tabela "phones")
    phone_ids = []
    for _ in range(STORES_QUANTITY):  # NUMBER_OF_STORES telefones, um por loja
        phone = generate_random_phone_number(forced_type='Commercial')
        result = session.execute(
            text("INSERT INTO phones (phone_type, number) VALUES (:phone_type, :number) RETURNING id"),
            phone
        )
        phone_ids.append(result.fetchone()[0])

    # --- População de Lojas (tabela "stores") ---
    store_ids = []
    
    

    for name in stores_names_used:
        store = {"name": f"{name}"}
        result = session.execute(
            text("INSERT INTO stores (name) VALUES (:name) RETURNING id"),
            store
        )
        store_ids.append(result.fetchone()[0])

    # --- Associação de Lojas aos Endereços (tabela "stores_addresses") ---
    for store_id, address_id in zip(store_ids, address_ids):
        session.execute(
            text("INSERT INTO stores_addresses (store_id, address_id) VALUES (:store_id, :address_id)"),
            {"store_id": store_id, "address_id": address_id}
        )

    # --- Associação de Lojas aos Telefones (tabela "phones_stores") ---
    for store_id, phone_id in zip(store_ids, phone_ids):
        session.execute(
            text("INSERT INTO phones_stores (store_id, phone_id) VALUES (:store_id, :phone_id)"),
            {"store_id": store_id, "phone_id": phone_id}
        )

    # --- População de Itens (tabela "items") ---
    def find_right_category(item:str):
        """find right category according item name"""
        category_name = str(df_items_and_categories[df_items_and_categories['nome_item'] == item]['categoria'].values[0])
        return category_ids[category_name]
    
    
    item_ids = []
    for item in items_used:

        item = {
            "name": f"{item}",
            "category_id": find_right_category(item)
        }
        result = session.execute(
            text("INSERT INTO items (name, category_id) VALUES (:name, :category_id) RETURNING id"),
            item
        )
        item_ids.append(result.fetchone()[0])

    
    # --- População do Inventário (tabela "inventory") ---
    inventory_entries = set()
    inserted = 0
    attempts_without_new = 0

    while inserted < 2000 and attempts_without_new < 200:
        item_id = random.choice(item_ids)
        size_id = random.choice(size_ids)
        store_id = random.choice(store_ids)
        entry = (item_id, size_id, store_id)
        
        if entry not in inventory_entries:
            inventory_entries.add(entry)
            inserted += 1
            attempts_without_new = 0  # Reseta o contador de tentativas sem sucesso
            
            quantity = random.randint(1, 500)
            session.execute(
                text("INSERT INTO inventory (item_id, size_id, store_id, quantity) VALUES (:item_id, :size_id, :store_id, :quantity)"),
                {"item_id": item_id, "size_id": size_id, "store_id": store_id, "quantity": quantity}
            )
        else:
            # Combinação já existente: incrementa o contador de tentativas sem sucesso
            attempts_without_new += 1

    # --- População de Preços (tabela "prices")
    # Para cada item, seleciona aleatoriamente de 1 a 4 tamanhos e insere um preço para cada combinação.
    price_entries = []
    for item_id in item_ids:
        num_sizes = random.randint(1, 4)
        selected_sizes = random.sample(size_ids, num_sizes)
        for size_id in selected_sizes:
            price_entries.append({
                "item_id": item_id,
                "size_id": size_id,
                "value": round(random.uniform(12.50, 1940.75), 2)
            })
    session.execute(
        text("INSERT INTO prices (item_id, size_id, value) VALUES (:item_id, :size_id, :value)"),
        price_entries
    )

    # Commit das alterações e fechamento da sessão
    session.commit()
    session.close()
    print("Data has been created successfully")

# Definição da DAG para inicialização dos dados
with DAG(
    dag_id='init_ecommerce',
    start_date=datetime(2025, 4, 2),
    schedule_interval="@once",
    catchup=False,
) as dag:
    init_task = PythonOperator(
        task_id='create_initial_data',
        python_callable=create_initial_data,
    )
