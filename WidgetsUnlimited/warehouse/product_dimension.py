from typing import Tuple, Dict
import pandas as pd
from datetime import date

from pandas.core.frame import DataFrame, Series, Index
from .warehouse_util import read_stage
from model.product_dim import ProductDimTable
from model.product import ProductTable
from model.product_supplier import ProductSupplierTable

# transformation mappings

product_dim_to_product_mapping = {
    "product_key" : "product_id",  # natural key -- Add index
    "name" : "product_name",
    "description" : "product_description",
    "category" : "product_category",
    "brand" : "product_brand",
    "unit_cost" : "product_unit_cost",
    "dimension_length" : "product_dimension_length",
    "dimension_width" : "product_dimension_width",
    "dimension_height" : "product_dimension_height",
    "introduced_date" : "product_introduced_date",
    "discontinued" : "product_discontinued",
    "no_longer_offered" : "product_no_longer_offered",
}


class ProductDimensionProcessor:
    """
    Transform the customer_dimension table in the mySQL star schema.

    The process_update method is called when a batch of incremental updates of source data is
    available in data warehouse staging area.

    The following naming conventions are used in the class:

    pandas DataFrames

    customer - staged customer data
    customer_address - staged customer address data
    customer_dim - mySQL star schema customer_dimension data

    pandas Indexes

    incremental_keys - All customer ids (natural key) present in batch
    update_keys - Customer ids already in the star schema
    new_keys - Customer ids not yet in the star schema
    """

    def __init__(self, connection=None):
        """
        Initialize CustomerDimensionProcessor
        :param connection: mySQL connection created by the warehouse.  None is used for test.
        """

        self._connection = connection
        self._dimension_table = ProductDimTable()
        self._next_surrogate_key = 1
        if connection:
            self._create_dimension()

    def process_update(self, batch_id: int) -> None:
        """
        Perform the following steps to update the product_dimension table for an ETL batch

        1) Read product and product_supplier files from stage area into dataframes
        2) Load dimension header columns of product_dim from mySQL to dataframe
        3) Compute new_keys and update_keys
        4) Compute insert and update product_dim records using transformations taking product, product_supplier
        and product_dim as inputs
        5) concatenate input and update product_dim records
        6) truncate product dimension table write concatenated records
        :param batch_id: Identifier for ETL process
        :return:None
        """

        product, product_supplier = read_stage(
            batch_id, [ProductTable(), ProductSupplierTable()]
        )

        prior_product_dim = self._read_dimension("product_key")
        update_keys = prior_product_dim.index
        new_keys = product.index.difference(update_keys)
        print(
            f"ProductDimensionProcessor: {len(new_keys) + len(update_keys)} unique product ids detected",
            end=" ",
        )
        print(f"(New: {len(new_keys)})", end=" ")
        print(f"(Updated: {len(update_keys)})")

        inserts = self._build_new_dimension(new_keys, product, product_supplier)
        updates = self._build_update_dimension(
            update_keys, prior_product_dim, product, product_supplier
        )

        self._truncate_dimension()
        self._write_dimension(inserts, "INSERT")
        self._next_surrogate_key += inserts.shape[0]

        print(
            f"ProductDimensionProcessor: {self._count_dimension()} total rows in product_dim table"
        )

    def _truncate_dimension(self):
        """ Truncate dimension prior to rewrite """

        cur = self._connection.cursor()
        cur.execute(f"TRUNCATE TABLE  {self._dimension_table.get_name()};")

    def _create_dimension(self):
        """Create an empty product_dimension on warehouse initialization."""

        cur = self._connection.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {self._dimension_table.get_name()};")
        cur.execute(self._dimension_table.get_create_sql_mysql())

    def _read_dimension(self, key_name: str) -> DataFrame:
        """
        Read all header columns amd natural key from product_dim

        :param key_name: Name of the index key
        :return: A dataframe of the result set, indexed by the key column
        """

        table_name = self._dimension_table.get_name()
        columns_list = ",".join([col for col in self._dimension_table.get_header_columns()])
        query = f"SELECT {key_name}, {columns_list} FROM {table_name});"
        dimension_df = pd.read_sql_query(query, self._connection)
        dimension_df = dimension_df.set_index(key_name, drop=False)

        return dimension_df

    def _write_dimension(self, customer_dim: DataFrame, operation: str) -> None:
        """
        Write a dataframe containing inserts or updates to the mySQL customer_dimension table.
        Convert the dataframe to a python list and use mysql-connector-python for bulk execution call.

        :param customer_dim: dataframe conforming to customer_dim schema
        :param operation: INSERT/REPLACE -- mirror mySQL verbs for insert/upsert
        :return: None
        """
        if customer_dim.shape[0] > 0:
            table = self._dimension_table
            table_name = table.get_name()
            column_names = ",".join(table.get_column_names())
            values_substitutions = ",".join(["%s"] * len(table.get_column_names()))
            cur = self._connection.cursor()
            rows = customer_dim.to_numpy().tolist()

            cur.executemany(
                f"{operation} INTO {table_name} ({column_names}) values ({values_substitutions})",
                rows,
            )

            if operation == "INSERT":
                operation_text = "inserts"
                rows_affected = cur.rowcount
            else:
                operation_text = "updates"
                rows_affected = cur.rowcount // 2
            print(
                f"CustomerDimensionProcessor: {rows_affected} {operation_text} written to {table_name} table"
            )

            self._connection.commit()

    def _count_dimension(self) -> int:
        """Return number of rows in dimension table"""
        table_name = self._dimension_table.get_name()
        cur = self._connection.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
        return cur.fetchone()[0]

    @staticmethod
    def product_transform(
            product_dim: DataFrame, product: DataFrame, product_supplier: DataFrame
    ) -> DataFrame:
        """
        Common transformations that apply to both inserts and updates.

        - simple column to column mapping
        - append number_of_suppliers

        :param product_dim:
        :param product:
        :param product_supplier: indexed by product_id
        :return: a product_dim dataframe refreshed with current values.

        """

        # simple copies from product to product_dim
        for k, v in product_dim_to_product_mapping.items():
            product_dim[k] = product[v]

        product_dim['number_of_suppliers'] = product_supplier.index.value_counts(sort=False)

        return product_dim

    def _build_new_dimension(
            self, new_keys: Index, product: DataFrame, product_supplier: DataFrame
    ) -> DataFrame:
        """
        Create and initialize new records for customer_dimension table in star schema

        :param new_keys: keys for records in batch not yet in star schema
        :param product: staged customer data
        :param product_supplier: staged customer address data
        :return: a customer_dim dataframe ready to be written to mySQL
        """

        if len(new_keys) == 0:
            return pd.DataFrame([])

        # restrict stage date to new_keys
        product = product.loc[new_keys]
        product_supplier = product_supplier.loc[new_keys]

        product_dim = pd.DataFrame(
            [], columns=ProductDimTable().get_column_names(), index=new_keys
        )

        # assign surrogate keys
        next_surrogate_key = self._next_surrogate_key
        num_inserts = len(new_keys)
        product_dim["surrogate_key"] = range(
            next_surrogate_key, next_surrogate_key + num_inserts
        )

        product_dim["effective_date"] = date(2020, 10, 10)
        product_dim["expiration_date"] = date(2099, 12, 31)
        product_dim["is_current_row"] = "Y"

        # apply common transformation
        product_dim = ProductDimensionProcessor.product_transform(
            product_dim, product, product_supplier
        )

        # conform output types
        product_dim = product_dim.astype(
            self._dimension_table.get_column_pandas_types()
        )

        return product_dim

    def _build_update_dimension(
            self,
            update_keys: Index,
            prior_product_dim: DataFrame,
            product: DataFrame,
            product_supplier: DataFrame,
    ) -> DataFrame:

        """
        Update existing records in customer_dimension table star schema

        :param update_keys: keys for records in batch already in star schema
        :param prior_product_dim:
        :param customer: staged customer data
        :param customer_address: staged customer address data
        :return: a customer_dim dataframe ready to be written to mySQL
        """

        if prior_product_dim.shape[0] == 0:
            return pd.DataFrame([])

        # restrict stage date to update_keys
        product = product.loc[update_keys]
        product_supplier = product_supplier.loc[update_keys]

        update_dim = ProductDimensionProcessor.product_transform(
            prior_product_dim, product, product_supplier
        )

        # conform output types
        update_dim = update_dim.astype(self._dimension_table.get_column_pandas_types())

        return update_dim
