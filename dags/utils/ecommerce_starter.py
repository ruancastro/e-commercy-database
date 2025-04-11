from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from faker import Faker
import random
from utils.phone_utils import generate_random_phone_number
from os import path
import pandas as pd
from utils.brazilian_address_complement import generate_brazilian_address_complement
from utils.store_email import generate_store_email
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
    """
    
    def __init__(self,DATABASE_URL,path_root_csv,stores_quantity,items_quantity,):


        self.root_df = pd.read_csv(path_root_csv)
        self.categories = self.root_df['categoria'].unique()
        self.items = self.root_df['nome_item'].unique()

        # unique_sizes = root_df['tamanhos_validos'].unique()
        self.valid_sizes = sorted(
            set(
                valor
                for item in self.root_df['tamanhos_validos'].unique()
                for valor in item.split(';')
            )
        )
        
        self.stores_names = [
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
        
        self.engine = create_engine(DATABASE_URL)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

        
        self.stores_quantity = stores_quantity
        self.items_quantity = items_quantity

        self.category_ids = None
        self.size_ids = None
        self.address_ids = None
        self.phone_ids = None
        self.store_ids = None
        self.item_ids = None
        self.items_sizes = None

        # HOW MUCH? 
        self.items_used = self.items[0:items_quantity]
        self.stores_names_used = self.stores_names[0:self.stores_quantity]

        self.fake = Faker('pt_BR')
 
    def create_initial_data(self):
        self.category_ids = self.populate_categories()
        self.size_ids = self.populate_sizes() #self.valid_sizes
        self.address_ids = self.populate_addresses_stores()  # O COMPLEMENTO TA XOXO
        self.phone_ids = self.populate_phones_stores() 
        self.store_ids = self.populate_stores()

        self.associate_addresses_to_stores()
        self.associate_phones_to_stores()

        self.item_ids = self.populate_items() # self.items_used
        self.items_sizes = self.populate_items_sizes()
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
                "street": self.fake.street_name(),
                "number": str(self.fake.building_number()),
                "complement": generate_brazilian_address_complement(),
                "neighborhood": self.fake.bairro(),
                "city": self.fake.city(),
                "state": self.fake.estado_sigla(),
                "zip_code": self.fake.postcode(),
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

            This method generates a predefined number of self.fake commercial phone numbers (one per store) using
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
            store = {"name": f"{name}", "email": f"{generate_store_email(name)}"}
            result = self.session.execute(
                text("INSERT INTO stores (name, email) VALUES (:name, :email) RETURNING id"),
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
        category_name = str(self.root_df[self.root_df['nome_item'] == item]['categoria'].values[0])
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
    
    def populate_items_sizes(self):
        """
        Inserts valid item-size associations into the 'items_sizes' table.

        This method filters items from the CSV that are present in self.items_used, maps item names and sizes 
        to their respective IDs, and inserts the associations into the 'items_sizes' table.

        Returns:
            list: A list of tuples (item_id, size_id) representing the inserted associations.
        """
        # Create dictionaries for quick lookup
        item_id_map = dict(zip(self.items_used, self.item_ids))  # Maps item name to item_id
        size_id_map = dict(zip(self.valid_sizes, self.size_ids))  # Maps size to size_id

        # List to store the inserted (item_id, size_id) pairs
        inserted_pairs = []

        # Filter the DataFrame to include only items in self.items_used
        df_filtered = self.root_df[self.root_df['nome_item'].isin(self.items_used)]

        # Iterate over each row in the filtered DataFrame
        for _, row in df_filtered.iterrows():
            item_name = row['nome_item']
            valid_sizes_str = row['tamanhos_validos']

            # Get the item_id for the current item
            item_id = item_id_map[item_name]

            # Split the valid sizes (semicolon-separated)
            valid_sizes = valid_sizes_str.split(';')

            # Insert each valid size association into the items_sizes table
            for size in valid_sizes:
                size_id = size_id_map[size]
                item_size = {
                    "item_id": item_id,
                    "size_id": size_id
                }
                self.session.execute(
                    text("INSERT INTO items_sizes (item_id, size_id) VALUES (:item_id, :size_id)"),
                    item_size
                )
                inserted_pairs.append((item_id, size_id))

        return inserted_pairs

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

    def populate_prices(self):
        """
        Populates the 'prices' table with random price entries for valid item-size combinations.

        This method uses the valid item-size associations (e.g., from a previous populate_items_sizes
        function or the items_sizes table) and assigns a random price between 12.50 and 1940.75 
        for each combination.

        Returns:
            None
        """

        price_entries = []
        for item_id, size_id in self.items_sizes:
            price_entries.append({
                "item_id": item_id,
                "size_id": size_id,
                "value": round(random.uniform(12.50, 1940.75), 2)
            })

        self.session.execute(
            text("INSERT INTO prices (item_id, size_id, value) VALUES (:item_id, :size_id, :value)"),
            price_entries
        )