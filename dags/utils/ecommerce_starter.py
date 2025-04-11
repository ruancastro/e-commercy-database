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


class EcommerceStarter:
    """
    A class responsible for initializing and populating the main tables of an e-commerce database.

    This class handles the creation and association of core entities such as categories, sizes,
    addresses, phone numbers, stores, items, inventory, and pricing. It establishes a connection to the 
    database using SQLAlchemy and manages data insertion through a session object.

    The quantity of data inserted can be customized using parameters like `stores_quantity` and 
    `items_quantity`. All reference data (such as item names and categories) is extracted from a
    CSV file provided at initialization.

    Attributes:
        engine (Engine): SQLAlchemy engine created from the given DATABASE_URL.
        session (Session): SQLAlchemy session used for executing transactions.
        stores_quantity (int): Number of stores to generate and insert.
        items_quantity (int): Number of items to insert into the database.
        categories (list): List of category names extracted from the CSV.
        items (list): List of item names extracted from the CSV.
        valid_sizes (list): List of valid sizes parsed from the CSV (e.g., ['P', 'M', 'G']).
        stores_names (list): Full list of possible store names.
        items_used (list): Subset of item names limited by `items_quantity`.
        stores_names_used (list): Subset of store names limited by `stores_quantity`.
        category_ids (dict): Mapping of category names to their corresponding inserted IDs.
        size_ids (list): List of inserted size IDs.
        address_ids (list): List of inserted address IDs linked to stores.
        phone_ids (list): List of inserted phone IDs linked to stores.
        store_ids (list): List of inserted store IDs.
        item_ids (list): List of inserted item IDs.
        items_sizes (list): List of tuples representing valid (item_id, size_id) associations.

    Args:
        DATABASE_URL (str): SQLAlchemy-compatible database connection string.
        path_root_csv (str): Path to the CSV file containing items, categories, and sizes.
        stores_quantity (int): Number of stores to generate.
        items_quantity (int): Number of items to use from the CSV data.
    """

    def __init__(
        self,
        DATABASE_URL,
        path_root_csv,
        stores_quantity,
        items_quantity,
    ):

        self.root_df = pd.read_csv(path_root_csv)
        self.categories = self.root_df["categoria"].unique()
        self.items = self.root_df["nome_item"].unique()

        self.valid_sizes = sorted(
            set(
                valor
                for item in self.root_df["tamanhos_validos"].unique()
                for valor in item.split(";")
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
            "Viva Estilo",
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
        self.stores_names_used = self.stores_names[0 : self.stores_quantity]

        self.fake = Faker("pt_BR")

    def create_initial_data(self):
        """
        Orchestrates the full data population process for the e-commerce database.

        This method calls all population and association methods in the correct order to initialize the
        database with realistic sample data. It includes categories, sizes, addresses, phones, stores,
        items, item-size associations, inventory, and pricing.

        After inserting all data, the changes are committed and the session is closed.

        Returns:
            None
        """
        self.category_ids = self.populate_categories()
        self.size_ids = self.populate_sizes()
        self.address_ids = self.populate_addresses_stores() 
        self.phone_ids = self.populate_phones_stores()
        self.store_ids = self.populate_stores()

        self.associate_addresses_to_stores()
        self.associate_phones_to_stores()

        self.item_ids = self.populate_items()  
        self.items_sizes = self.populate_items_sizes()
        self.populate_inventory()
        self.populate_prices() 

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

            category = {"name": category}
            result = self.session.execute(
                text("INSERT INTO categories (name) VALUES (:name) RETURNING id"),
                category,
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
                {"size": size},
            )
            size_ids.append(result.fetchone()[0])
        return size_ids

    def populate_addresses_stores(self):
        """
        Inserts randomly generated store addresses into the 'addresses' table and retrieves their IDs.

        This method generates a predefined number of fake addresses (one per store) using the 'faker' library.
        It also uses a helper function to generate realistic Brazilian address complements. Each address is 
        inserted into the database using a parameterized query, and the assigned ID is collected.

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
                "country": "Brasil",
            }
            result = self.session.execute(
                text(
                    "INSERT INTO addresses (street, number, complement, neighborhood, city, state, zip_code, country) "
                    "VALUES (:street, :number, :complement, :neighborhood, :city, :state, :zip_code, :country) RETURNING id"
                ),
                address,
            )
            address_ids.append(result.fetchone()[0])
        return address_ids

    def populate_phones_stores(self):
        """
        Inserts randomly generated store phone numbers into the 'phones' table and retrieves their IDs.

        This method generates a predefined number of commercial phone numbers (one per store) using the
        'generate_random_phone_number' function. Each phone number is inserted into the database using a 
        parameterized query, and the corresponding ID is collected.

        Returns:
            list: A list of integers representing the IDs assigned to the inserted phone numbers.
        """
        phone_ids = []
        for _ in range(self.stores_quantity):  # NUMBER_OF_STORES telefones, um por loja
            phone = generate_random_phone_number(forced_type="Commercial")
            result = self.session.execute(
                text(
                    "INSERT INTO phones (phone_type, number) VALUES (:phone_type, :number) RETURNING id"
                ),
                phone,
            )
            phone_ids.append(result.fetchone()[0])
        return phone_ids

    def populate_stores(self):
        """
        Inserts predefined store names and generated emails into the 'stores' table and retrieves their IDs.

        This method iterates through a list of predefined store names, generates an email address for each store
        using the 'generate_store_email' function, and inserts the name and email into the database using a 
        parameterized SQL query. The ID of each inserted store is then collected.

        Returns:
            list: A list of integers representing the IDs assigned to the inserted stores.
        """
        store_ids = []
        for name in self.stores_names_used:
            store = {"name": f"{name}", "email": f"{generate_store_email(name)}"}
            result = self.session.execute(
                text(
                    "INSERT INTO stores (name, email) VALUES (:name, :email) RETURNING id"
                ),
                store,
            )
            store_ids.append(result.fetchone()[0])
        return store_ids

    def associate_addresses_to_stores(self):
        """
        Associates each store with its corresponding address in the 'stores_addresses' table.

        This method iterates through paired lists of store IDs and address IDs, inserting a record
        into the 'stores_addresses' junction table for each store-address pair.

        Returns:
            None
        """
        for store_id, address_id in zip(self.store_ids, self.address_ids):
            self.session.execute(
                text(
                    "INSERT INTO stores_addresses (store_id, address_id) VALUES (:store_id, :address_id)"
                ),
                {"store_id": store_id, "address_id": address_id},
            )

    def associate_phones_to_stores(self):
        """
        Associates each store with its corresponding phone number in the 'phones_stores' table.

        This method iterates through paired lists of store IDs and phone IDs, inserting a record
        into the 'phones_stores' junction table for each store-phone pair.

        Returns:
            None
        """
        for store_id, phone_id in zip(self.store_ids, self.phone_ids):
            self.session.execute(
                text(
                    "INSERT INTO phones_stores (store_id, phone_id) VALUES (:store_id, :phone_id)"
                ),
                {"store_id": store_id, "phone_id": phone_id},
            )

    def __find_right_category(self, item: str):
        """
        Retrieves the category ID associated with a given item name.

        This method looks up the item's category in the dataset and returns the corresponding
        category ID from the pre-populated `category_ids` dictionary.

        Args:
            item (str): The name of the item to look up.

        Returns:
            int: The ID of the category associated with the item.
        """
        category_name = str(
            self.root_df[self.root_df["nome_item"] == item]["categoria"].values[0]
        )
        return self.category_ids[category_name]

    def populate_items(self):
        """
        Inserts predefined item names into the 'items' table and retrieves their assigned IDs.

        This method iterates through a list of item names, determines the corresponding category ID
        for each item using the internal mapping, inserts the item into the database, and collects
        the generated ID.

        Returns:
            list: A list of integers representing the IDs assigned to the inserted items.
        """
        item_ids = []
        for item in self.items_used:

            item = {"name": f"{item}", "category_id": self.__find_right_category(item)}
            result = self.session.execute(
                text(
                    "INSERT INTO items (name, category_id) VALUES (:name, :category_id) RETURNING id"
                ),
                item,
            )
            item_ids.append(result.fetchone()[0])
        return item_ids

    def populate_items_sizes(self):
        """
        Inserts valid item-size associations into the 'items_sizes' table.

        This method filters the dataset to include only items listed in 'self.items_used',
        maps each item name and its corresponding valid sizes to their respective IDs,
        and inserts these associations into the 'items_sizes' table.

        Returns:
            list: A list of tuples (item_id, size_id) representing the inserted associations.
        """
        # Create dictionaries for quick lookup
        item_id_map = dict(
            zip(self.items_used, self.item_ids)
        )  # Maps item name to item_id
        size_id_map = dict(zip(self.valid_sizes, self.size_ids))  # Maps size to size_id

        # List to store the inserted (item_id, size_id) pairs
        inserted_pairs = []

        # Filter the DataFrame to include only items in self.items_used
        df_filtered = self.root_df[self.root_df["nome_item"].isin(self.items_used)]

        # Iterate over each row in the filtered DataFrame
        for _, row in df_filtered.iterrows():
            item_name = row["nome_item"]
            valid_sizes_str = row["tamanhos_validos"]

            # Get the item_id for the current item
            item_id = item_id_map[item_name]

            # Split the valid sizes (semicolon-separated)
            valid_sizes = valid_sizes_str.split(";")

            # Insert each valid size association into the items_sizes table
            for size in valid_sizes:
                size_id = size_id_map[size]
                item_size = {"item_id": item_id, "size_id": size_id}
                self.session.execute(
                    text(
                        "INSERT INTO items_sizes (item_id, size_id) VALUES (:item_id, :size_id)"
                    ),
                    item_size,
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
                    text(
                        "INSERT INTO inventory (item_id, size_id, store_id, quantity) VALUES (:item_id, :size_id, :store_id, :quantity)"
                    ),
                    {
                        "item_id": item_id,
                        "size_id": size_id,
                        "store_id": store_id,
                        "quantity": quantity,
                    },
                )
            else:
                # Combinação já existente: incrementa o contador de tentativas sem sucesso
                attempts_without_new += 1

    def populate_prices(self):
        """
        Populates the 'prices' table with random price entries for valid item-size combinations.

        This method iterates over valid (item_id, size_id) pairs — typically generated by a previous call
        to populate_items_sizes — and assigns a random price between 12.50 and 1940.75 to each.

        Returns:
            None
        """

        price_entries = []
        for item_id, size_id in self.items_sizes:
            price_entries.append(
                {
                    "item_id": item_id,
                    "size_id": size_id,
                    "value": round(random.uniform(12.50, 1940.75), 2),
                }
            )

        self.session.execute(
            text(
                "INSERT INTO prices (item_id, size_id, value) VALUES (:item_id, :size_id, :value)"
            ),
            price_entries,
        )
