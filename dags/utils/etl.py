import pandas as pd
import numpy as np
from sqlalchemy import (
    create_engine,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
from psycopg2 import extras
from sqlalchemy.dialects.postgresql import insert
from abc import ABC, abstractmethod
from sqlalchemy.orm import Session
from dag_create_ecommerce_tables_olap import FactSales, FactInventory, DimTime, DimSizes, DimStores, DimItems, DimCustomers
Base = declarative_base()


class ETLBase(ABC):
    """
    Abstract base class for ETL, containing common methods for both initial and incremental loads.
    """

    def __init__(self, oltp_url, olap_url):
        self.oltp_url = oltp_url
        self.olap_url = olap_url
        self.oltp_engine = create_engine(self.oltp_url)
        self.olap_engine = create_engine(self.olap_url)

    def extract_oltp(self):
        """
        Extracts data from the OLTP database.
        """
        df_customers_addresses = pd.read_sql(
            "SELECT * FROM customers_addresses;", self.oltp_engine
        )
        df_phones_customers = pd.read_sql(
            "SELECT * FROM phones_customers;", self.oltp_engine
        )
        df_customers = pd.read_sql("SELECT * FROM customers;", self.oltp_engine)
        df_purchases_status = pd.read_sql(
            "SELECT * FROM purchases_status;", self.oltp_engine
        )
        df_purchases = pd.read_sql("SELECT * FROM purchases", self.oltp_engine)
        df_purchases_items = pd.read_sql(
            "SELECT * FROM purchases_items;", self.oltp_engine
        )
        df_addresses = pd.read_sql("SELECT * FROM addresses;", self.oltp_engine)
        df_phones = pd.read_sql("SELECT * FROM phones;", self.oltp_engine)
        df_items = pd.read_sql("SELECT * FROM items;", self.oltp_engine)
        df_sizes = pd.read_sql("SELECT * FROM sizes;", self.oltp_engine)
        df_items_sizes = pd.read_sql("SELECT * FROM items_sizes;", self.oltp_engine)
        df_stores = pd.read_sql("SELECT * FROM stores;", self.oltp_engine)
        df_prices = pd.read_sql("SELECT * FROM prices;", self.oltp_engine)
        df_categories = pd.read_sql("SELECT * FROM categories;", self.oltp_engine)
        df_inventory = pd.read_sql("SELECT * FROM inventory;", self.oltp_engine)
        df_stores_addresses = pd.read_sql(
            "SELECT * FROM stores_addresses;", self.oltp_engine
        )
        df_phones_stores = pd.read_sql("SELECT * FROM phones_stores;", self.oltp_engine)

        return {
            "customers_addresses": df_customers_addresses,
            "phones_customers": df_phones_customers,
            "customers": df_customers,
            "purchases_status": df_purchases_status,
            "purchases_items": df_purchases_items,
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
            "phones_stores": df_phones_stores,
        }

    def transform_common_dimensions(self, extracted_data):
        """
        Common transformations for dimensions, used in both initial and incremental loads.
        """
        # df_customers_addresses = extracted_data["customers_addresses"]
        # df_phones_customers = extracted_data["phones_customers"]
        # df_phones_stores = extracted_data["phones_stores"]
        df_customers = extracted_data["customers"]
        df_addresses = extracted_data["addresses"]
        df_items = extracted_data["items"]
        df_sizes = extracted_data["sizes"]
        df_items_sizes = extracted_data["items_sizes"]
        df_stores = extracted_data["stores"]
        df_categories = extracted_data["categories"]
        df_stores_addresses = extracted_data["stores_addresses"]

        dim_customers = df_customers

        dim_items = df_items
        dim_items.rename(columns={"id": "item_id"}, inplace=True)
        dim_items = pd.merge(
            dim_items, df_categories, left_on="category_id", right_on="id", how="left"
        )
        dim_items.rename(
            columns={"name_x": "name", "name_y": "category_name"}, inplace=True
        )
        dim_items.drop(
            columns={"category_id", "id", "created_at_x", "created_at_y"}, inplace=True
        )

        dim_sizes = df_sizes.rename(columns={"id": "size_id"})
        dim_sizes.drop(columns={"created_at"}, inplace=True)

        dim_stores = df_stores.rename(columns={"id": "store_id"})
        dim_stores.drop(columns={"email"}, inplace=True)
        dim_stores = pd.merge(
            dim_stores,
            df_stores_addresses,
            left_on="store_id",
            right_on="store_id",
            how="left",
        )
        dim_stores = pd.merge(
            dim_stores, df_addresses, left_on="address_id", right_on="id", how="left"
        )
        dim_stores.drop(
            columns={
                "address_id",
                "id",
                "street",
                "number",
                "complement",
                "neighborhood",
                "country",
            },
            inplace=True,
        )
        state_to_region = {
            "AC": "Norte",
            "AL": "Nordeste",
            "AP": "Norte",
            "AM": "Norte",
            "BA": "Nordeste",
            "CE": "Nordeste",
            "DF": "Centro-Oeste",
            "ES": "Sudeste",
            "GO": "Centro-Oeste",
            "MA": "Nordeste",
            "MT": "Centro-Oeste",
            "MS": "Centro-Oeste",
            "MG": "Sudeste",
            "PA": "Norte",
            "PB": "Nordeste",
            "PR": "Sul",
            "PE": "Nordeste",
            "PI": "Nordeste",
            "RJ": "Sudeste",
            "RN": "Nordeste",
            "RS": "Sul",
            "RO": "Norte",
            "RR": "Norte",
            "SC": "Sul",
            "SP": "Sudeste",
            "SE": "Nordeste",
            "TO": "Norte",
        }
        dim_stores["region"] = dim_stores["state"].map(state_to_region)
        dim_stores.drop(
            columns=["created_at", "created_at_x", "created_at_y"], inplace=True
        )

        return dim_customers, dim_items, dim_sizes, dim_stores

    @abstractmethod
    def extract(self):
        pass


class ETLInitial(ETLBase):
    """
    Class for initial load of the OLAP database.
    """

    def extract(self):
        return self.extract_oltp()

    def transform(self, extracted_data):
        df_purchases_status = extracted_data["purchases_status"]
        df_purchases = extracted_data["purchases"]
        df_purchases_items = extracted_data["purchases_items"]
        df_items_sizes = extracted_data["items_sizes"]
        df_prices = extracted_data["prices"]
        df_inventory = extracted_data["inventory"]

        # Common transformations for dimensions
        dim_customers, dim_items, dim_sizes, dim_stores = (
            self.transform_common_dimensions(extracted_data)
        )

        # Dimension dim_time with date_id
        dim_time = pd.DataFrame()
        dim_time["date"] = pd.to_datetime(df_purchases["order_date"], errors="coerce")
        dim_time = dim_time.drop_duplicates(subset=["date"]).reset_index(drop=True)
        dim_time["date_id"] = dim_time.index + 1
        dim_time["day"] = dim_time["date"].dt.day
        dim_time["month"] = dim_time["date"].dt.month
        dim_time["quarter"] = ((dim_time["month"] - 1) // 3) + 1
        dim_time["year"] = dim_time["date"].dt.year
        dim_time["is_weekend"] = dim_time["date"].dt.dayofweek.isin([5, 6])
        dim_time = dim_time[
            ["date_id", "date", "day", "month", "quarter", "year", "is_weekend"]
        ]
        if dim_time["date"].isna().any():
            raise ValueError(
                "Null or invalid values found in 'order_date' while creating dim_time"
            )

        # Add the current date to dim_time for fact_inventory
        current_date = pd.to_datetime(datetime.now().date())
        if current_date not in dim_time["date"].values:
            new_date_id = dim_time["date_id"].max() + 1 if not dim_time.empty else 1
            temp_dim_time = pd.DataFrame(
                {
                    "date_id": [new_date_id],
                    "date": [current_date],
                    "day": [current_date.day],
                    "month": [current_date.month],
                    "quarter": [((current_date.month - 1) // 3) + 1],
                    "year": [current_date.year],
                    "is_weekend": [current_date.dayofweek in [5, 6]],
                }
            )
            dim_time = pd.concat([dim_time, temp_dim_time], ignore_index=True)

        fact_sales = df_purchases
        fact_sales.rename(columns={"id": "purchase_id"}, inplace=True)
        fact_sales["order_date"] = pd.to_datetime(
            fact_sales["order_date"], errors="coerce"
        )
        fact_sales = pd.merge(
            fact_sales,
            dim_time[["date_id", "date"]],
            left_on="order_date",
            right_on="date",
            how="inner",
        )
        fact_sales.drop(columns=["order_date", "created_at", "date"], inplace=True)
        fact_sales = pd.merge(
            fact_sales, df_purchases_status, on="purchase_id", how="inner"
        )
        fact_sales.rename(columns={"status": "purchase_status"}, inplace=True)
        fact_sales = pd.merge(
            fact_sales, df_purchases_items, on="purchase_id", how="inner"
        )
        fact_sales = pd.merge(
            fact_sales, df_prices, on=["item_id", "size_id"], how="inner"
        )
        fact_sales["line_value"] = fact_sales["value"] * fact_sales["quantity"]
        fact_sales.drop(columns=["value"], inplace=True)
        fact_sales.rename(columns={"quantity": "quantity_sold"}, inplace=True)
        column_order = [
            "purchase_id",
            "item_id",
            "size_id",
            "customer_id",
            "store_id",
            "date_id",
            "purchase_status",
            "line_value",
            "quantity_sold",
        ]
        fact_sales = fact_sales[column_order]
        if "date_id" not in fact_sales.columns or fact_sales["date_id"].isna().any():
            raise ValueError("Issue propagating date_id to fact_sales")

        # Fact table fact_inventory
        fact_inventory = df_items_sizes
        fact_inventory = pd.merge(
            fact_inventory, df_inventory, on=["item_id", "size_id"], how="right"
        )
        fact_inventory["date_id"] = dim_time[dim_time["date"] == current_date][
            "date_id"
        ].iloc[0]
        fact_inventory.rename(columns={"quantity": "quantity_in_stock"}, inplace=True)
        fact_inventory = fact_inventory[
            ["item_id", "size_id", "store_id", "date_id", "quantity_in_stock"]
        ]
        if (
            "date_id" not in fact_inventory.columns
            or fact_inventory["date_id"].isna().any()
        ):
            raise ValueError("Issue including date_id in fact_inventory")

        transformed_data = {
            "dim_customers": dim_customers,
            "dim_items": dim_items,
            "dim_sizes": dim_sizes,
            "dim_stores": dim_stores,
            "dim_time": dim_time,
            "fact_sales": fact_sales,
            "fact_inventory": fact_inventory,
        }
        return transformed_data

    def load(self, transformed_data):
        """
        Loads the transformed data into the OLAP database for initial population.
        Drops existing tables with CASCADE and recreates the schema using SQLAlchemy ORM.
        Synchronizes sequences for autoincrement fields after loading data.
        """
        load_order = [
            "dim_time",
            "dim_customers",
            "dim_items",
            "dim_sizes",
            "dim_stores",
            "fact_sales",
            "fact_inventory",
        ]

        # Passo 1: Exclui e recria as tabelas
        Base.metadata.drop_all(self.olap_engine)
        print("All existing tables dropped successfully with CASCADE.")

        Base.metadata.create_all(self.olap_engine)
        print("OLAP schema recreated successfully.")

        # Passo 2: Carrega os dados
        for table_name in load_order:
            df = transformed_data.get(table_name)
            if df is not None and not df.empty:
                df.to_sql(
                    table_name,
                    self.olap_engine,
                    if_exists="append",
                    index=False,
                    method="multi",
                )
                print(f"Table {table_name} successfully loaded into OLAP.")
            else:
                print(f"Table {table_name} is empty or was not provided.")

        with Session(self.olap_engine) as session:
            # Synchronize the dim_time.date_id sequence
            max_date_id = session.execute("SELECT MAX(date_id) FROM dim_time").scalar()
            max_date_id = max_date_id if max_date_id is not None else 0
            session.execute(f"SELECT setval('dim_time_date_id_seq', {max_date_id + 1})")
            print(f"dim_time_date_id_seq sequence adjusted to {max_date_id + 1}")

            # Synchronize other sequences, if necessary
            # dim_customers.customer_id
            max_customer_id = session.execute(
                "SELECT MAX(customer_id) FROM dim_customers"
            ).scalar()
            max_customer_id = max_customer_id if max_customer_id is not None else 0
            session.execute(
                f"SELECT setval('dim_customers_customer_id_seq', {max_customer_id + 1})"
            )
            print(
                f"dim_customers, customer_id_seq sequence adjusted to {max_customer_id + 1}"
            )

            # dim_items.item_id
            max_item_id = session.execute("SELECT MAX(item_id) FROM dim_items").scalar()
            max_item_id = max_item_id if max_item_id is not None else 0
            session.execute(
                f"SELECT setval('dim_items_item_id_seq', {max_item_id + 1})"
            )
            print(f"dim_items_item_id_seq sequence adjusted to {max_item_id + 1}")

            # dim_sizes.size_id
            max_size_id = session.execute("SELECT MAX(size_id) FROM dim_sizes").scalar()
            max_size_id = max_size_id if max_size_id is not None else 0
            session.execute(
                f"SELECT setval('dim_sizes_size_id_seq', {max_size_id + 1})"
            )
            print(f"dim_sizes_size_id_seq sequence adjusted to {max_size_id + 1}")

            # dim_stores.store_id
            max_store_id = session.execute(
                "SELECT MAX(store_id) FROM dim_stores"
            ).scalar()
            max_store_id = max_store_id if max_store_id is not None else 0
            session.execute(
                f"SELECT setval('dim_stores_store_id_seq', {max_store_id + 1})"
            )
            print(f"dim_stores_store_id_seq sequence adjusted to {max_store_id + 1}")

            session.commit()
        print("All sequences were successfully synchronized.")


class ETLIncremental(ETLBase):

    def extract(self, last_execution_date):
        """Extracts incremental data from the OLTP based on the last execution date."""
        queries = {
            "customers": f"SELECT * FROM customers WHERE created_at > '{last_execution_date}'",
            "items": f"SELECT * FROM items WHERE created_at > '{last_execution_date}'",
            "sizes": f"SELECT * FROM sizes WHERE created_at > '{last_execution_date}'",
            "stores": f"SELECT * FROM stores WHERE created_at > '{last_execution_date}'",
            "categories": f"SELECT * FROM categories WHERE created_at > '{last_execution_date}'",
            "purchases": f"SELECT * FROM purchases WHERE created_at > '{last_execution_date}'",
            "purchases_status": f"SELECT * FROM purchases_status WHERE created_at > '{last_execution_date}'",
            "purchases_items": f"SELECT * FROM purchases_items WHERE created_at > '{last_execution_date}'",
            "prices": f"SELECT * FROM prices WHERE created_at > '{last_execution_date}'",
            "items_sizes": f"SELECT * FROM items_sizes WHERE created_at > '{last_execution_date}'",
            "inventory": f"SELECT * FROM inventory WHERE created_at > '{last_execution_date}'",
            "stores_addresses": f"SELECT * FROM stores_addresses WHERE created_at > '{last_execution_date}'",
            "addresses": f"SELECT * FROM addresses WHERE created_at > '{last_execution_date}'",
        }
        extracted_data = {
            key: pd.read_sql(query, self.oltp_engine) for key, query in queries.items()
        }
        if extracted_data["purchases"].empty:
            raise ValueError("No new records found to process")
        return extracted_data

    def transform_dimensions(self, extracted_data):
        """Transform the extracted data into dimension tables."""

        df_purchases = extracted_data["purchases"]

        dim_customers, dim_items, dim_sizes, dim_stores = (
            self.transform_common_dimensions(extracted_data)
        )

        # dim_time
        new_dates = pd.to_datetime(df_purchases["order_date"]).dt.date.unique()
        existing_dates_df = pd.read_sql("SELECT date FROM dim_time", self.olap_engine)
        existing_dates_df["date"] = pd.to_datetime(
            existing_dates_df["date"], errors="coerce"
        )

        existing_dates = existing_dates_df["date"].dt.date.unique()
        new_dates = [d for d in new_dates if d not in existing_dates]
        dim_time_new = pd.DataFrame(
            {
                "date": new_dates,
                "day": [d.day for d in new_dates],
                "month": [d.month for d in new_dates],
                "quarter": [((d.month - 1) // 3) + 1 for d in new_dates],
                "year": [d.year for d in new_dates],
                "is_weekend": [d.weekday() >= 5 for d in new_dates],
            }
        )

        return {
            "dim_customers": dim_customers,
            "dim_items": dim_items,
            "dim_sizes": dim_sizes,
            "dim_stores": dim_stores,
            "dim_time": dim_time_new,
        }

    def load_dimensions(self, transformed_dimensions):
        """Load the dimensions into the OLAP using UPSERT or INSERT ON CONFLICT."""

        # dim_time
        dim_time_new = transformed_dimensions["dim_time"]
        if not dim_time_new.empty:
            with Session(self.olap_engine) as session:
                for _, row in dim_time_new.iterrows():
                    values = {
                        "date": row["date"],
                        "day": row["day"],
                        "month": row["month"],
                        "quarter": row["quarter"],
                        "year": row["year"],
                        "is_weekend": row["is_weekend"],
                    }
                    stmt = (
                        insert(DimTime)
                        .values(**values)
                        .on_conflict_do_nothing(index_elements=["date"])
                    )
                    session.execute(stmt)
                session.commit()

        # UPSERT
        dimensions = [
            (
                "dim_customers",
                DimCustomers,
                "customer_id",
                ["full_name", "email", "created_at"],
            ),
            ("dim_items", DimItems, "item_id", ["name", "category_name"]),
            ("dim_sizes", DimSizes, "size_id", ["size"]),
            (
                "dim_stores",
                DimStores,
                "store_id",
                ["name", "city", "state", "zip_code", "region"],
            ),
        ]
        for table_name, model, pk, update_cols in dimensions:
            df = transformed_dimensions[table_name]
            if not df.empty:
                with Session(self.olap_engine) as session:
                    for _, row in df.iterrows():
                        data = row.to_dict()
                        stmt = (
                            insert(model)
                            .values(**data)
                            .on_conflict_do_update(
                                index_elements=[pk],
                                set_={col: data[col] for col in update_cols},
                            )
                        )
                        session.execute(stmt)
                    session.commit()

    def transform_facts(self, extracted_data):
        """Transform the extracted data into fact tables."""
        dim_time = pd.read_sql("SELECT date_id, date FROM dim_time", self.olap_engine)
        dim_time["date"] = pd.to_datetime(dim_time["date"]).dt.date

        # fact_sales
        df_purchases = extracted_data["purchases"]
        df_purchases_status = extracted_data["purchases_status"]
        df_purchases_items = extracted_data["purchases_items"]
        df_prices = extracted_data["prices"]

        df_purchases["order_date"] = pd.to_datetime(df_purchases["order_date"]).dt.date

        fact_sales = pd.merge(
            df_purchases,
            dim_time[["date", "date_id"]],
            left_on="order_date",
            right_on="date",
            how="left",
        )
        if fact_sales["date_id"].isnull().any():
            raise ValueError(
                "Algumas datas de pedido não possuem date_id correspondente."
            )
        fact_sales = fact_sales.drop(columns=["date", "order_date", "created_at"])
        fact_sales.rename(columns={"id": "purchase_id"}, inplace=True)

        fact_sales = pd.merge(
            fact_sales, df_purchases_status, on="purchase_id", how="inner"
        )
        fact_sales = pd.merge(
            fact_sales, df_purchases_items, on="purchase_id", how="inner"
        )
        fact_sales = pd.merge(
            fact_sales, df_prices, on=["item_id", "size_id"], how="inner"
        )
        fact_sales = fact_sales.drop(columns=["created_at_x", "created_at_y"])
        fact_sales["line_value"] = fact_sales["value"] * fact_sales["quantity"]
        fact_sales = fact_sales.rename(
            columns={
                "quantity": "quantity_sold",
                "status": "purchase_status",
                "quantity": "quantity_sold",
            }
        )
        fact_sales = fact_sales[
            [
                "purchase_id",
                "item_id",
                "size_id",
                "customer_id",
                "store_id",
                "date_id",
                "purchase_status",
                "line_value",
                "quantity_sold",
            ]
        ]

        # fact_inventory
        current_date = datetime.now().date()
        current_date_id = dim_time[dim_time["date"] == current_date]["date_id"].iloc[0]
        df_items_sizes = extracted_data["items_sizes"]
        df_inventory = extracted_data["inventory"]

        fact_inventory = pd.merge(
            df_items_sizes, df_inventory, on=["item_id", "size_id"], how="right"
        )
        fact_inventory["date_id"] = current_date_id
        fact_inventory = fact_inventory.rename(
            columns={"quantity": "quantity_in_stock"}
        )
        fact_inventory = fact_inventory[
            ["item_id", "size_id", "store_id", "date_id", "quantity_in_stock"]
        ]

        return {"fact_sales": fact_sales, "fact_inventory": fact_inventory}

    def load_facts(self, transformed_facts):
        """Load the fact tables into the OLAP using INSERT ON CONFLICT with Session, in batches."""
        batch_size = 1000

        # fact_sales
        df_sales = transformed_facts["fact_sales"]
        if not df_sales.empty:
            # Replace nan with None in the customer_id column (which is nullable).
            df_sales["customer_id"] = df_sales["customer_id"].replace(np.nan, None)

            with Session(self.olap_engine) as session:
                for start in range(0, len(df_sales), batch_size):
                    batch = df_sales[start : start + batch_size]
                    values = [row.to_dict() for _, row in batch.iterrows()]
                    stmt = (
                        insert(FactSales)
                        .values(values)
                        .on_conflict_do_nothing(
                            index_elements=["purchase_id", "item_id", "size_id"]
                        )
                    )
                    session.execute(stmt)
                session.commit()
            print(f"fact_sales successfully loaded: {len(df_sales)} rows.")

        # fact_inventory
        df_inventory = transformed_facts["fact_inventory"]
        if not df_inventory.empty:
            with Session(self.olap_engine) as session:
                for start in range(0, len(df_inventory), batch_size):
                    batch = df_inventory[start : start + batch_size]
                    values = [row.to_dict() for _, row in batch.iterrows()]
                    stmt = (
                        insert(FactInventory)
                        .values(values)
                        .on_conflict_do_nothing(
                            index_elements=["item_id", "size_id", "store_id", "date_id"]
                        )
                    )
                    session.execute(stmt)
                session.commit()
            print(f"fact_inventory successfully loaded: {len(df_inventory)} rows.")


if __name__ == "__main__":
    OLTP_URL = "postgresql+psycopg2://oltp:ecommerce123@localhost:5433/ecommerce_oltp"
    OLAP_URL = (
        "postgresql+psycopg2://olap:ecommerce123@localhost:5434/ecommerce_olap"  # local
    )

    # Instantiate ETLIncremental
    etl = ETLIncremental(oltp_url=OLTP_URL, olap_url=OLAP_URL)

    last_execution_date = "2025-04-24 00:00:00"

    # Task 1: extract_op (equivalent to extract_task in the DAG)
    print("Starting extract_op task...")
    extracted_data = etl.extract(last_execution_date)
    print("Extract completed. Extracted data:")
    for key, df in extracted_data.items():
        print(f"{key}: {df.shape[0]} rows")

    # Task 2: transform_dimensions_op (equivalent to transform_dimensions_task in the DAG)
    print("\nStarting transform_dimensions_op task...")
    transformed_dimensions = etl.transform_dimensions(extracted_data)
    print("Dimensions transformation completed. Transformed data:")
    for key, df in transformed_dimensions.items():
        print(f"{key}: {df.shape[0]} rows")

    # Task 3: load_dimensions_op (equivalent to load_dimensions_task in the DAG)
    print("\nStarting load_dimensions_op task...")
    etl.load_dimensions(transformed_dimensions)
    print("Dimensions loading completed.")

    # Task 4: transform_facts_op (equivalent to transform_facts_task in the DAG)
    print("\nStarting transform_facts_op task...")
    transformed_facts = etl.transform_facts(extracted_data)
    print("Facts transformation completed. Transformed data:")
    for key, df in transformed_facts.items():
        print(f"{key}: {df.shape[0]} rows")

    # Task 5: load_facts_op (equivalent to load_facts_task in the DAG)
    print("\nStarting load_facts_op task...")
    etl.load_facts(transformed_facts)
    print("Facts loading completed.")

    print("\nETLIncremental pipeline completed successfully!")
