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
ITEMS_QUANTITY = 40 #MAX 100

dag_dir = path.dirname(path.abspath(__file__))
csv_path = path.join(dag_dir, "utils", "items_and_categories.csv")
root_csv = read_csv(csv_path)

categories = root_csv['categoria'].unique()

items = root_csv['nome_item'].unique()

unique_sizes = root_csv['tamanhos_validos'].unique()
valid_sizes = sorted(
    set(
        valor
        for item in root_csv['tamanhos_validos'].unique()
        for valor in item.split(';')
    )
)

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



# How much ?

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

class EcommerceStarter():
    """
    A class responsible for initializing and populating the main tables of an e-commerce database.

    This class manages the creation and association of essential entities such as categories, sizes,
    addresses, phone numbers, stores, items, inventory, and pricing. It establishes a database
    connection via SQLAlchemy using a provided database URL and uses sessions to execute SQL operations.

    The amount of data to populate can be controlled through parameters such as `stores_quantity` and
    `items_quantity`. It uses lists of items, categories, and store names provided at instantiation.

    Attributes:
        engine (Engine): SQLAlchemy engine created from the given DATABASE_URL.
        session (Session): SQLAlchemy session used to interact with the database.
        stores_quantity (int): Number of stores to be created.
        items_quantity (int): Number of items to be inserted.
        categories (list): List of category names used in item classification.
        items (list): List of item names to be inserted into the database.
        valid_sizes (list): List of valid sizes to be stored in the sizes table.
        stores_names (list): List of possible store names.
        address_complements (list): List of optional address complements.
        items_used (list): Subset of items to be used based on `items_quantity`.
        stores_names_used (list): Subset of store names based on `stores_quantity`.
        category_ids (dict): Maps category names to their corresponding IDs after insertion.
        size_ids (list): List of size IDs inserted into the database.
        address_ids (list): List of address IDs associated with stores.
        phone_ids (list): List of phone IDs associated with stores.
        store_ids (list): List of store IDs inserted into the database.
        item_ids (list): List of item IDs inserted into the database.

    Args:
        DATABASE_URL (str): The database connection string used to create the engine and session.
        stores_quantity (int): Number of stores to generate and insert.
        items_quantity (int): Number of items to generate and insert.
        categories (list): List of all categories used for item classification.
        items (list): List of all possible item names.
        valid_sizes (list): List of valid size strings (e.g., 'S', 'M', 'L').
        stores_names (list): List of potential store names to be used.
        address_complements (list): Optional complements to be randomly assigned to addresses.
    """
    
    def __init__(self,DATABASE_URL,stores_quantity,items_quantity,categories,items,valid_sizes,stores_names,address_complements):
        
        self.engine = create_engine(DATABASE_URL)
        self.session = sessionmaker(bind=engine)
        
        self.stores_quantity = stores_quantity
        self.items_quantity = items_quantity
        self.categories = categories
        self.items = items
        self.valid_sizes = valid_sizes
        self.stores_names = stores_names
        self.address_complements = address_complements

        self.category_ids = None
        self.size_ids = None
        self.address_ids = None
        self.phone_ids = None
        self.store_ids = None
        self.item_ids = None

        # HOW MUCH? 
        self.items_used = self.items[0:items_quantity]
        self.stores_names_used = self.stores_names[0:self.stores_quantity]
    
    def create_initial_data(self):
        self.category_ids = self.populate_categories()
        self.size_ids = self.populate_sizes()
        self.address_ids = self.populate_addresses_stores()  # O COMPLEMENTO TA XOXO
        self.phone_ids = self.populate_phones_stores() 
        self.store_ids = self.populate_stores()

        self.associate_addresses_to_stores()
        self.associate_phones_to_stores()

        self.item_ids = self.populate_items()
        
        self.populate_inventory()
        self.populate_prices() # TA ERRADO

        # Commit das alterações e fechamento da sessão
        self.session.commit()
        self.session.close()
        print("Data has been created successfully")

    def populate_categories(self):
        """
        Inserts predefined categories into the 'categories' table and retrieves their assigned IDs.

        This method iterates through a predefined list of category names, inserts each one into the
        database using a parameterized SQL query, and collects the generated ID for each inserted category.

        Returns:
            dict: A dictionary where each key is a category name and each value is the corresponding ID
                returned by the database.
        """


        category_ids = {}
        for category in self.categories:
            
            category = {'name': category}
            result = self.session.execute(
                text("INSERT INTO categories (name) VALUES (:name) RETURNING id"),
                category
            )
            category_ids[category["name"]] = result.scalar()
        return category_ids

    def populate_sizes(self):
        """
        Inserts a list of valid sizes into the 'sizes' table and retrieves their assigned IDs.

        This method iterates through a predefined list of valid sizes, inserts each one into the
        database using a parameterized SQL query, and collects the generated ID for each inserted size.

        Returns:
            list: A list of integers representing the IDs assigned to the inserted sizes.
        """

        size_ids = []
        for size in self.valid_sizes:
            result = self.session.execute(
                text("INSERT INTO sizes (size) VALUES (:size) RETURNING id"),
                {"size": size}
            )
            size_ids.append(result.fetchone()[0])
        return size_ids

    def populate_addresses_stores(self): # O COMPLEMENTO TA XOXO
        """
        Inserts randomly generated store addresses into the 'addresses' table and retrieves their IDs.

        This method generates a predefined number of fake addresses (one per store) using the 'faker' library
        along with a list of possible address complements. Each address is inserted into the database, and
        the corresponding ID is collected.

        Returns:
            list: A list of integers representing the IDs assigned to the inserted addresses.
        """
        address_ids = []
        for _ in range(self.stores_quantity):  # NUMBER_OF_STORES endereços, um por loja
            address = {
                "street": fake.street_name(),
                "number": str(fake.building_number()),
                "complement": random.choice(self.address_complements) if random.random() >= 0.3 else None,
                "neighborhood": fake.bairro(),
                "city": fake.city(),
                "state": fake.estado_sigla(),
                "zip_code": fake.postcode(),
                "country": "Brasil"
            }
            result = self.session.execute(
                text("INSERT INTO addresses (street, number, complement, neighborhood, city, state, zip_code, country) "
                    "VALUES (:street, :number, :complement, :neighborhood, :city, :state, :zip_code, :country) RETURNING id"),
                address
            )
            address_ids.append(result.fetchone()[0])
        return address_ids
    
    def populate_phones_stores(self):
            """
            Inserts randomly generated store phone numbers into the 'phones' table and retrieves their IDs.

            This method generates a predefined number of fake commercial phone numbers (one per store) using
            the 'generate_random_phone_number' function. Each phone number is inserted into the database, and
            the corresponding ID is collected.

            Returns:
                list: A list of integers representing the IDs assigned to the inserted phone numbers.
            """
            phone_ids = []
            for _ in range(self.stores_quantity):  # NUMBER_OF_STORES telefones, um por loja
                phone = generate_random_phone_number(forced_type='Commercial')
                result = self.session.execute(
                    text("INSERT INTO phones (phone_type, number) VALUES (:phone_type, :number) RETURNING id"),
                    phone
                )
                phone_ids.append(result.fetchone()[0])
            return phone_ids

    def populate_stores(self):
        """
        Inserts predefined store names into the 'stores' table and retrieves their IDs.

        This method iterates through a list of predefined store names, inserts each one into the database
        using a parameterized SQL query, and collects the generated ID for each inserted store.

        Returns:
            list: A list of integers representing the IDs assigned to the inserted stores.
        """
        store_ids = []
        for name in self.stores_names_used:
            store = {"name": f"{name}"}
            result = self.session.execute(
                text("INSERT INTO stores (name) VALUES (:name) RETURNING id"),
                store
            )
            store_ids.append(result.fetchone()[0])
        return store_ids

    def associate_addresses_to_stores(self):
        """
        Associates each store with its corresponding address in the 'stores_addresses' table.

        This method iterates through paired lists of store IDs and address IDs, inserting a relationship
        entry into the junction table 'stores_addresses' for each pair.

        Returns:
            None
        """
        for store_id, address_id in zip(self.store_ids, self.address_ids):
            self.session.execute(
                text("INSERT INTO stores_addresses (store_id, address_id) VALUES (:store_id, :address_id)"),
                {"store_id": store_id, "address_id": address_id}
            )

    def associate_phones_to_stores(self):
        """
        Associates each store with its corresponding phone number in the 'phones_stores' table.

        This method iterates through paired lists of store IDs and phone IDs, inserting a relationship
        entry into the junction table 'phones_stores' for each pair.

        Returns:
            None
        """
        for store_id, phone_id in zip(self.store_ids, self.phone_ids):
            self.session.execute(
                text("INSERT INTO phones_stores (store_id, phone_id) VALUES (:store_id, :phone_id)"),
                {"store_id": store_id, "phone_id": phone_id}
            )

    def __find_right_category(self,item:str):
        """
        Retrieves the corresponding category ID for a given item name.

        This method searches the dataset for the category name associated with the provided item,
        then returns the corresponding category ID from the pre-populated dictionary of category IDs.

        Args:
            item (str): The name of the item to look up.

        Returns:
            int: The ID of the category associated with the item.
        """
        category_name = str(root_csv[root_csv['nome_item'] == item]['categoria'].values[0])
        return self.category_ids[category_name]

    def populate_items(self):
        """
        Inserts predefined items into the 'items' table and retrieves their assigned IDs.

        This method iterates through a predefined list of item names, determines the correct category ID 
        for each item, inserts the item into the database, and collects the generated ID.

        Returns:
            list: A list of integers representing the IDs assigned to the inserted items.
        """
        item_ids = []
        for item in self.items_used:

            item = {
                "name": f"{item}",
                "category_id": self.__find_right_category(item)
            }
            result = self.session.execute(
                text("INSERT INTO items (name, category_id) VALUES (:name, :category_id) RETURNING id"),
                item
            )
            item_ids.append(result.fetchone()[0])
        return item_ids

    def populate_inventory(self):
        """
        Populates the 'inventory' table with random combinations of items, sizes, and stores.

        This method inserts up to 2000 unique entries into the inventory table, ensuring that no 
        duplicate combinations of (item_id, size_id, store_id) are inserted. For each unique combination, 
        a random quantity between 1 and 500 is assigned.

        The loop breaks if 200 consecutive attempts fail to generate a new unique combination.

        Returns:
            None
        """
        inventory_entries = set()
        inserted = 0
        attempts_without_new = 0

        while inserted < 2000 and attempts_without_new < 200:
            item_id = random.choice(self.item_ids)
            size_id = random.choice(self.size_ids)
            store_id = random.choice(self.store_ids)
            entry = (item_id, size_id, store_id)
            
            if entry not in inventory_entries:
                inventory_entries.add(entry)
                inserted += 1
                attempts_without_new = 0  # Reseta o contador de tentativas sem sucesso
                
                quantity = random.randint(1, 500)
                self.session.execute(
                    text("INSERT INTO inventory (item_id, size_id, store_id, quantity) VALUES (:item_id, :size_id, :store_id, :quantity)"),
                    {"item_id": item_id, "size_id": size_id, "store_id": store_id, "quantity": quantity}
                )
            else:
                # Combinação já existente: incrementa o contador de tentativas sem sucesso
                attempts_without_new += 1

    def populate_prices(self): # TA ERRADO
        """
        Populates the 'prices' table with random price entries for item-size combinations.

        For each item, this method randomly selects between 1 and 4 different sizes
        and assigns a random price between 12.50 and 1940.75 for each combination.
        
        Returns:
            None
        """
        price_entries = []
        for item_id in self.item_ids:
            num_sizes = random.randint(1, 4)
            selected_sizes = random.sample(self.size_ids, num_sizes)
            for size_id in selected_sizes:
                price_entries.append({
                    "item_id": item_id,
                    "size_id": size_id,
                    "value": round(random.uniform(12.50, 1940.75), 2)
                })
        self.session.execute(
            text("INSERT INTO prices (item_id, size_id, value) VALUES (:item_id, :size_id, :value)"),
            price_entries
        )





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
