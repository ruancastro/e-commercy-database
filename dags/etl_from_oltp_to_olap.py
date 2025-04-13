import pandas as pd
from sqlalchemy import create_engine


# Extraindo dados de uma tabela (ex.: customers)
class ETL():
    """
    """
    def __init__(self,oltp_url,olap_url):
        self.oltp_url = oltp_url
        self.olap_url = olap_url
        
        self.oltp_engine = create_engine(self.oltp_url)

    def extract(self):
        """
        Extracts data from the OLTP database and returns it as a dictionary of DataFrames.

            This method connects to the OLTP database using the provided engine and extracts data
            from all relevant tables. Each table's data is stored in a pandas DataFrame, and the
            DataFrames are returned in a dictionary where the keys are descriptive names of the tables.

            Returns:
                dict: A dictionary mapping table names to their corresponding pandas DataFrames.
                    The keys are strings representing table names, and the values are pandas DataFrames
                    containing the data extracted from the respective tables. The dictionary contains
                    the following keys:
                    - "customers_addresses": DataFrame with data from the customers_addresses table.
                    - "phones_customers": DataFrame with data from the phones_customers table.
                    - "customers": DataFrame with data from the customers table.
                    - "purchases_status": DataFrame with data from the purchases_status table.
                    - "purchases": DataFrame with data from the purchases table.
                    - "addresses": DataFrame with data from the addresses table.
                    - "phones": DataFrame with data from the phones table.
                    - "items": DataFrame with data from the items table.
                    - "sizes": DataFrame with data from the sizes table.
                    - "stores": DataFrame with data from the stores table.
                    - "prices": DataFrame with data from the prices table.
                    - "categories": DataFrame with data from the categories table.
                    - "inventory": DataFrame with data from the inventory table.
                    - "stores_addresses": DataFrame with data from the stores_addresses table.
                    - "phones_stores": DataFrame with data from the phones_stores table.

            Raises:
                sqlalchemy.exc.SQLAlchemyError: If there is an error connecting to the database
                    or executing the SQL queries.
            """
        df_customers_addresses = pd.read_sql("SELECT * FROM customers_addresses;", self.oltp_engine)
        df_phones_customers = pd.read_sql("SELECT * FROM phones_customers;",self.oltp_engine)
        df_customers = pd.read_sql("SELECT * FROM customers;",self.oltp_engine)
        
        df_purchases_status = pd.read_sql("SELECT * FROM purchases_status;",self.oltp_engine)
        df_purchases = pd.read_sql("SELECT * FROM purchases", self.oltp_engine)
        df_addresses = pd.read_sql("SELECT * FROM addresses;",self.oltp_engine)
        df_phones = pd.read_sql("SELECT * FROM phones;",self.oltp_engine)

        df_items = pd.read_sql("SELECT * FROM items;",self.oltp_engine)
        df_sizes = pd.read_sql("SELECT * FROM sizes;",self.oltp_engine)
        df_stores = pd.read_sql("SELECT * FROM stores;",self.oltp_engine)

        df_prices = pd.read_sql("SELECT * FROM prices;",self.oltp_engine)
        df_categories = pd.read_sql("SELECT * FROM categories;",self.oltp_engine)
        df_inventory = pd.read_sql("SELECT * FROM inventory;",self.oltp_engine)
        df_stores_addresses = pd.read_sql("SELECT * FROM stores_addresses;",self.oltp_engine)
        df_phones_stores = pd.read_sql("SELECT * FROM phones_stores;",self.oltp_engine)

        return {
        "customers_addresses": df_customers_addresses,
        "phones_customers": df_phones_customers,
        "customers": df_customers,
        "purchases_status": df_purchases_status,
        "purchases": df_purchases,
        "addresses": df_addresses,
        "phones": df_phones,
        "items": df_items,
        "sizes": df_sizes,
        "stores": df_stores,
        "prices": df_prices,
        "categories": df_categories,
        "inventory": df_inventory,
        "stores_addresses": df_stores_addresses,
        "phones_stores": df_phones_stores
    }

    def transform(self, extracted_data: dict):
        """Transforms the extracted OLTP data into a format suitable for the OLAP database.

        This method takes the extracted data from the OLTP database, which is provided as a dictionary
        of pandas DataFrames, and performs the necessary transformations to prepare the data for loading
        into the OLAP database. The transformations include joining tables, aggregating data, and mapping
        the data to the OLAP schema (dimensions and facts).

        Args:
            extracted_data (dict): A dictionary containing the extracted DataFrames from the OLTP database.
                The dictionary keys correspond to table names, and the values are pandas DataFrames with the
                data from those tables. Expected keys are:
                - "customers_addresses"
                - "phones_customers"
                - "customers"
                - "purchases_status"
                - "purchases"
                - "addresses"
                - "phones"
                - "items"
                - "sizes"
                - "stores"
                - "prices"
                - "categories"
                - "inventory"
                - "stores_addresses"
                - "phones_stores"

        Returns:
            dict: A dictionary mapping OLAP table names to their corresponding transformed pandas DataFrames.
                The keys are strings representing OLAP table names (e.g., "dim_customers", "fact_sales"),
                and the values are pandas DataFrames containing the transformed data ready for loading.

        Raises:
            KeyError: If an expected key is missing from the extracted_data dictionary.
            ValueError: If the data in the DataFrames does not meet the expected format or constraints.
        """
        # Carregar os DataFrames do dicionário extracted_data em variáveis locais
        df_customers_addresses = extracted_data["customers_addresses"]
        df_phones_customers = extracted_data["phones_customers"]
        df_customers = extracted_data["customers"]
        df_purchases_status = extracted_data["purchases_status"]
        df_purchases = extracted_data["purchases"]
        df_addresses = extracted_data["addresses"]
        df_phones = extracted_data["phones"]
        df_items = extracted_data["items"]
        df_sizes = extracted_data["sizes"]
        df_stores = extracted_data["stores"]
        df_prices = extracted_data["prices"]
        df_categories = extracted_data["categories"]
        df_inventory = extracted_data["inventory"]
        df_stores_addresses = extracted_data["stores_addresses"]
        df_phones_stores = extracted_data["phones_stores"]

        #Transform 
        df_dim_customers = df_customers

        df_dim_items = df_items
        df_dim_items.rename(columns={'id': 'item_id'}, inplace = True)
        df_dim_items = pd.merge(df_dim_items,df_categories,left_on = 'category_id', right_on = 'id', how = 'left')
        df_dim_items.rename(columns={'name_x': 'name', 'name_y': 'category_name'}, inplace = True)
        df_dim_items.drop(columns = {'category_id','id'}, inplace = True)

        transformed_data = {
            "dim_customers": None,
            "dim_items": None,
            "dim_sizes": None,
            "dim_stores": None,
            "dim_time": None,
            "fact_sales": None,
            "fact_inventory": None
        }

        return transformed_data

if __name__ == "__main__":
    OLTP_URL = "postgresql+psycopg2://oltp:ecommerce123@localhost:5433/ecommerce_oltp"
    # OLTP_URL = "postgresql+psycopg2://oltp:ecommerce123@postgres_oltp:5432/ecommerce_oltp"
    
    OLAP_URL = ''

    etl = ETL(oltp_url=OLTP_URL, olap_url=OLAP_URL)
    extracted_data = etl.extract()
    transformed_data = etl.transform(extracted_data)
    print("hey")
