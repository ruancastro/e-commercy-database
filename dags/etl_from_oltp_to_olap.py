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
                    - "items_sizes": DataFrame with data from the items_sizes table.
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
        
        df_purchases_status = pd.read_sql("SELECT * FROM purchases_status;", self.oltp_engine)
        df_purchases = pd.read_sql("SELECT * FROM purchases",  self.oltp_engine)
        df_addresses = pd.read_sql("SELECT * FROM addresses;", self.oltp_engine)
        df_phones = pd.read_sql("SELECT * FROM phones;", self.oltp_engine)

        df_items = pd.read_sql("SELECT * FROM items;", self.oltp_engine)
        df_sizes = pd.read_sql("SELECT * FROM sizes;", self.oltp_engine)
        df_items_sizes = pd.read_sql("SELECT * FROM items_sizes;",  self.oltp_engine)
        df_stores = pd.read_sql("SELECT * FROM stores;", self.oltp_engine)

        df_prices = pd.read_sql("SELECT * FROM prices;", self.oltp_engine)
        df_categories = pd.read_sql("SELECT * FROM categories;", self.oltp_engine)
        df_inventory = pd.read_sql("SELECT * FROM inventory;", self.oltp_engine)
        df_stores_addresses = pd.read_sql("SELECT * FROM stores_addresses;", self.oltp_engine)
        df_phones_stores = pd.read_sql("SELECT * FROM phones_stores;", self.oltp_engine)
        

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
        "items_sizes": df_items_sizes,
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
        df_items_sizes = extracted_data["items_sizes"]
        df_stores = extracted_data["stores"]
        df_prices = extracted_data["prices"]
        df_categories = extracted_data["categories"]
        df_inventory = extracted_data["inventory"]
        df_stores_addresses = extracted_data["stores_addresses"]
        df_phones_stores = extracted_data["phones_stores"]

        #Transform 
        dim_customers = df_customers

        dim_items = df_items
        dim_items.rename(columns={'id': 'item_id'}, inplace = True)
        dim_items = pd.merge(dim_items,df_categories,left_on = 'category_id', right_on = 'id', how = 'left')
        dim_items.rename(columns={'name_x': 'name', 'name_y': 'category_name'}, inplace = True)
        dim_items.drop(columns = {'category_id','id'}, inplace = True)

        dim_sizes = df_sizes.rename(columns={'id':'size_id'})

        dim_stores = df_stores.rename(columns={'id':'store_id'})
        dim_stores.drop(columns={'email'},inplace=True)
        dim_stores = pd.merge(dim_stores,df_stores_addresses,left_on='store_id',right_on='store_id',how='left')
        dim_stores = pd.merge(dim_stores,df_addresses,left_on='address_id',right_on='id',how='left')
        dim_stores.drop(columns={'address_id','id','street','number','complement','neighborhood','country'},inplace=True)
        state_to_region = {
            'AC': 'North',
            'AL': 'North East',
            'AP': 'North',
            'AM': 'North',
            'BA': 'North East',
            'CE': 'North East',
            'DF': 'Central-West',
            'ES': 'South East',
            'GO': 'Central-West',
            'MA': 'North East',
            'MT': 'Central-West',
            'MS': 'Central-West',
            'MG': 'South East',
            'PA': 'North',
            'PB': 'North East',
            'PR': 'South',
            'PE': 'North East',
            'PI': 'North East',
            'RJ': 'South East',
            'RN': 'North East',
            'RS': 'South',
            'RO': 'North',
            'RR': 'North',
            'SC': 'South',
            'SP': 'South East',
            'SE': 'North East',
            'TO': 'North'
        }
        dim_stores['region'] = dim_stores['state'].map(state_to_region)
        
        dim_time = pd.DataFrame()
        dim_time['date'] = pd.to_datetime(df_purchases['order_date'], errors='coerce')
        dim_time = dim_time.drop_duplicates().reset_index(drop=True)
        dim_time['date_id'] = dim_time.index + 1
        dim_time = dim_time[['date_id', 'date']]

        dim_time['day'] = dim_time['date'].dt.day
        dim_time['month'] = dim_time['date'].dt.month
        dim_time['quarter'] = ((dim_time['month'] - 1) // 3) + 1
        dim_time['year'] = dim_time['date'].dt.year
        dim_time['is_weekend'] = dim_time['date'].dt.dayofweek.isin([5, 6])

        fact_inventory = df_items_sizes
        fact_inventory = pd.merge(fact_inventory,df_inventory,on=['item_id','size_id'],how='right')
        fact_inventory.rename(columns={'quantity': 'quantity_in_stock'}, inplace= True)
        #Continue aqui depois


        fact_sales = df_purchases
        fact_sales.rename(columns={'id':'purchase_id'},inplace=True)
        fact_sales['order_date'] = pd.to_datetime(fact_sales['order_date'],errors='coerce')
        fact_sales = pd.merge(fact_sales,dim_time,left_on='order_date',right_on='date', how='inner')
        fact_sales.drop(columns=['order_date','created_at','date','day','month','quarter','year','is_weekend'],inplace=True)
        fact_sales = pd.merge(fact_sales,df_purchases_status,on='purchase_id',how='inner')
        fact_sales.rename(columns={'status':'purchase_status'}, inplace=True)
        #ainda falta, total_value e quantity_sold 
        
        
        # Faça a filtragem por null, nan etc (so montei ate agora)
        # Verifique os tipos
        
        transformed_data = {
            "dim_customers": dim_customers,
            "dim_items": dim_items,
            "dim_sizes": dim_sizes,
            "dim_stores": dim_stores,
            "dim_time": dim_time,
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
