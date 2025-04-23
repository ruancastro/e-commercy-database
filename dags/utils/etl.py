import pandas as pd
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    ForeignKey,
    Float,
    TIMESTAMP,
    Date,
    CHAR,
    PrimaryKeyConstraint,
    CheckConstraint,
    Boolean,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
from abc import ABC, abstractmethod

Base = declarative_base()


class FactSales(Base):
    __tablename__ = "fact_sales"
    purchase_id = Column(Integer, primary_key=True, nullable=False)
    item_id = Column(
        Integer, ForeignKey("dim_items.item_id"), primary_key=True, nullable=False
    )
    size_id = Column(
        Integer, ForeignKey("dim_sizes.size_id"), primary_key=True, nullable=False
    )
    customer_id = Column(
        Integer, ForeignKey("dim_customers.customer_id"), nullable=True
    )
    store_id = Column(Integer, ForeignKey("dim_stores.store_id"), nullable=False)
    date_id = Column(Integer, ForeignKey("dim_time.date_id"), nullable=False)
    purchase_status = Column(String(20), nullable=False)
    line_value = Column(Float, nullable=False)
    quantity_sold = Column(Integer, nullable=False)
    customer = relationship("DimCustomers", back_populates="sales")
    __table_args__ = (
        CheckConstraint("line_value >= 0", name="check_line_value_not_negative"),
        CheckConstraint("quantity_sold >= 0", name="check_quantity_sold_not_negative"),
        CheckConstraint(
            "purchase_status IN ('Pending', 'Sent', 'Delivered', 'Canceled')",
            name="check_valid_status",
        ),
    )


class FactInventory(Base):
    __tablename__ = "fact_inventory"
    item_id = Column(Integer, ForeignKey("dim_items.item_id"), nullable=False)
    size_id = Column(Integer, ForeignKey("dim_sizes.size_id"), nullable=False)
    store_id = Column(Integer, ForeignKey("dim_stores.store_id"), nullable=False)
    date_id = Column(Integer, ForeignKey("dim_time.date_id"), nullable=False)
    quantity_in_stock = Column(Integer, nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint("item_id", "size_id", "store_id", "date_id"),
        CheckConstraint(
            "quantity_in_stock >= 0", name="quantity_in_stock_not_negative"
        ),
    )


class DimTime(Base):
    __tablename__ = "dim_time"
    date_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    date = Column(Date, nullable=False, unique=True)
    day = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    year = Column(Integer, nullable=False)
    is_weekend = Column(Boolean, nullable=False)
    __table_args__ = (UniqueConstraint("date", name="unique_date"),)


class DimSizes(Base):
    __tablename__ = "dim_sizes"
    size_id = Column(Integer, primary_key=True, nullable=False)
    size = Column(String(10), nullable=False)


class DimStores(Base):
    __tablename__ = "dim_stores"
    store_id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(50), nullable=False)
    city = Column(String(50))
    state = Column(CHAR(2), nullable=False)
    region = Column(String(20), nullable=False)
    zip_code = Column(String(10), nullable=False)
    __table_args__ = (
        CheckConstraint(
            "state IN ('AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO')",
            name="check_valid_state",
        ),
        CheckConstraint(
            "zip_code ~ '^[0-9]{5}(-?[0-9]{3})?$'", name="check_zip_code_format"
        ),
        CheckConstraint(
            "region IN ('Norte', 'Nordeste', 'Centro-Oeste', 'Sudeste', 'Sul')",
            name="check_region",
        ),
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
        df_customers_addresses = extracted_data["customers_addresses"]
        df_phones_customers = extracted_data["phones_customers"]
        df_customers = extracted_data["customers"]
        df_addresses = extracted_data["addresses"]
        df_items = extracted_data["items"]
        df_sizes = extracted_data["sizes"]
        df_items_sizes = extracted_data["items_sizes"]
        df_stores = extracted_data["stores"]
        df_categories = extracted_data["categories"]
        df_stores_addresses = extracted_data["stores_addresses"]
        df_phones_stores = extracted_data["phones_stores"]

        dim_customers = df_customers

        dim_items = df_items
        dim_items.rename(columns={"id": "item_id"}, inplace=True)
        dim_items = pd.merge(
            dim_items, df_categories, left_on="category_id", right_on="id", how="left"
        )
        dim_items.rename(
            columns={"name_x": "name", "name_y": "category_name"}, inplace=True
        )
        dim_items.drop(columns={"category_id", "id"}, inplace=True)

        dim_sizes = df_sizes.rename(columns={"id": "size_id"})

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

        return dim_customers, dim_items, dim_sizes, dim_stores

    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def transform(self, extracted_data):
        pass

    @abstractmethod
    def load(self, transformed_data):
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

        Base.metadata.drop_all(self.olap_engine)
        print("All existing tables dropped successfully with CASCADE.")

        Base.metadata.create_all(self.olap_engine)
        print("OLAP schema recreated successfully.")

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


class ETLIncremental(ETLBase):
    """
    Class for incremental load of the OLAP database.
    """

    def extract(self, last_execution_date=None):
        # Extract data from OLTP (only deltas)
        extracted_data = self.extract_oltp()
        for key, df in extracted_data.items():
            if "created_at" in df.columns and last_execution_date:
                extracted_data[key] = df[
                    df["created_at"] > pd.to_datetime(last_execution_date)
                ]

        df_dim_time = pd.read_sql(
            "SELECT date_id, date FROM dim_time;", self.olap_engine
        )
        extracted_data["dim_time"] = df_dim_time

        return extracted_data

    def transform(self, extracted_data):
        df_purchases_status = extracted_data["purchases_status"]
        df_purchases = extracted_data["purchases"]
        df_purchases_items = extracted_data["purchases_items"]
        df_items_sizes = extracted_data["items_sizes"]
        df_prices = extracted_data["prices"]
        df_inventory = extracted_data["inventory"]
        df_dim_time = extracted_data["dim_time"]

        dim_customers, dim_items, dim_sizes, dim_stores = (
            self.transform_common_dimensions(extracted_data)
        )

        new_dates = pd.to_datetime(
            df_purchases["order_date"], errors="coerce"
        ).dt.date.drop_duplicates()
        new_dates_df = pd.DataFrame(
            {
                "date": new_dates,
                "day": new_dates.apply(lambda x: x.day if pd.notnull(x) else None),
                "month": new_dates.apply(lambda x: x.month if pd.notnull(x) else None),
                "quarter": new_dates.apply(
                    lambda x: ((x.month - 1) // 3) + 1 if pd.notnull(x) else None
                ),
                "year": new_dates.apply(lambda x: x.year if pd.notnull(x) else None),
                "is_weekend": new_dates.apply(
                    lambda x: x.weekday() in [5, 6] if pd.notnull(x) else None
                ),
            }
        )
        new_dates_df = new_dates_df[new_dates_df["date"].notna()]
        new_dates_df = new_dates_df[~new_dates_df["date"].isin(df_dim_time["date"])]
        dim_time = pd.concat([df_dim_time, new_dates_df], ignore_index=True)

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

        current_date = pd.to_datetime(datetime.now().date())
        if current_date not in dim_time["date"].values:
            temp_dim_time = pd.DataFrame(
                {
                    "date": [current_date],
                    "day": [current_date.day],
                    "month": [current_date.month],
                    "quarter": [((current_date.month - 1) // 3) + 1],
                    "year": [current_date.year],
                    "is_weekend": [current_date.dayofweek in [5, 6]],
                }
            )
            dim_time = pd.concat([dim_time, temp_dim_time], ignore_index=True)

        fact_inventory = df_items_sizes
        fact_inventory = pd.merge(
            fact_inventory, df_inventory, on=["item_id", "size_id"], how="right"
        )
        fact_inventory = pd.merge(
            fact_inventory,
            dim_time[["date_id", "date"]],
            left_on=pd.to_datetime(current_date).date(),
            right_on="date",
            how="left",
        )
        fact_inventory.drop(columns=["date"], inplace=True)
        fact_inventory.rename(columns={"quantity": "quantity_in_stock"}, inplace=True)
        fact_inventory = fact_inventory[
            ["item_id", "size_id", "store_id", "date_id", "quantity_in_stock"]
        ]

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
        load_order = [
            "dim_time",
            "dim_customers",
            "dim_items",
            "dim_sizes",
            "dim_stores",
            "fact_sales",
            "fact_inventory",
        ]

        for table_name in load_order:
            df = transformed_data.get(table_name)
            if df is not None and not df.empty:
                if table_name == "dim_time":
                    existing_dates = pd.read_sql(
                        "SELECT date FROM dim_time", self.olap_engine
                    )["date"]
                    new_dates = df[~df["date"].isin(existing_dates)]
                    if not new_dates.empty:
                        new_dates.to_sql(
                            table_name,
                            self.olap_engine,
                            if_exists="append",
                            index=False,
                            method="multi",
                        )
                else:
                    df.to_sql(
                        table_name,
                        self.olap_engine,
                        if_exists="append",
                        index=False,
                        method="multi",
                    )
                print(f"Table {table_name} successfully updated in OLAP.")
            else:
                print(f"Table {table_name} is empty or was not provided.")
