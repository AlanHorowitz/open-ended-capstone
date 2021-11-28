import pandas as pd
import logging
from datetime import date

from pandas.core.frame import DataFrame, Index
from .warehouse_util import read_stage
from WidgetsUnlimited.model.store_dim import StoreDimTable
from WidgetsUnlimited.model.store import StoreTable
from WidgetsUnlimited.model.store_location import StoreLocationTable

from .dimension_processor import DimensionProcessor

logger = logging.getLogger(__name__)

# transformation mappings

product_dim_to_product_mapping = {
    "product_key": "product_id",  # natural key -- Add index
    "name": "product_name",
    "description": "product_description",
    "category": "product_category",
    "brand": "product_brand",
    "unit_cost": "product_unit_cost",
    "dimension_length": "product_dimension_length",
    "dimension_width": "product_dimension_width",
    "dimension_height": "product_dimension_height",
    "introduced_date": "product_introduced_date",
    "discontinued": "product_discontinued",
    "no_longer_offered": "product_no_longer_offered",
}


class ProductDimensionProcessor(DimensionProcessor):
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
        super().__init__(connection, StoreDimTable())
        self._next_surrogate_key = 1

    def process_update(self, batch_id: int) -> None:
        """
        Perform the following steps to update the product_dimension table for an ETL batch

        1) Read store and store_location files from stage area into dataframes
        2) Load dimension header columns of product_dim from mySQL to dataframe
        3) Compute new_keys and update_keys
        4) Compute insert and update product_dim records using transformations taking store, store_location
        and product_dim as inputs
        5) concatenate input and update product_dim records
        6) truncate store dimension table write concatenated records
        :param batch_id: Identifier for ETL process
        :return:None
        """

        store, store_location = read_stage(
            batch_id, [StoreTable(), StoreLocationTable()]
        )
        incremental_keys = store.index.union(store_location.index).unique()

        if incremental_keys.size == 0:
            logger.debug("0 unique store ids detected")
            return

        prior_store_dim = self._read_dimension("store_key", incremental_keys)
        update_keys = prior_store_dim.index
        new_keys = incremental_keys.difference(update_keys)
        logger.debug(f"{len(incremental_keys)} unique store ids detected (New: {len(new_keys)}) "
                     f"(Updated: {len(update_keys)})")

        inserts = self._build_new_dimension(new_keys, store, store_location)
        updates = self._build_update_dimension(
            update_keys, prior_store_dim, store, store_location
        )

        self._write_table(None, inserts, "INSERT")
        self._next_surrogate_key += inserts.shape[0]
        self._write_table(None, updates, "REPLACE")

        logger.debug("%d total rows in store_dim table", self._count_dimension())

    def _read_dimension(self, key_name: str, key_values: Index) -> DataFrame:
        """
        Read rows from the customer_dimension table using a key filter

        :param key_name: Name of the filter key
        :param key_values: Filter values
        :return: A dataframe of the result set, indexed by the key column
        """

        table_name = self._dimension_table.get_name()
        key_values_list = ",".join(str(k) for k in key_values)
        query = f"SELECT * FROM {table_name} WHERE {key_name} IN ({key_values_list});"
        dimension_df = pd.read_sql_query(query, self._connection)
        dimension_df = dimension_df.set_index(key_name, drop=False)

        return dimension_df

    def _count_dimension(self) -> int:
        """Return number of rows in dimension table"""
        table_name = self._dimension_table.get_name()
        cur = self._connection.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
        return cur.fetchone()[0]

    @staticmethod
    def dimension_transform(
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

        product_dim["number_of_suppliers"] = product_supplier.index.value_counts(
            sort=False
        )
        product_dim["percent_returns"] = 0

        return product_dim

    def _build_new_dimension(
            self, new_keys: Index, store: DataFrame, store_location: DataFrame
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
        store = store.loc[new_keys]
        store_location = store_location.loc[new_keys]

        store_dim = pd.DataFrame(
            [], columns=StoreDimTable().get_column_names(), index=new_keys
        )

        # assign surrogate keys
        next_surrogate_key = self._next_surrogate_key
        num_inserts = len(new_keys)
        store_dim["surrogate_key"] = range(
            next_surrogate_key, next_surrogate_key + num_inserts
        )

        store_dim["effective_date"] = date(2020, 10, 10)
        store_dim["expiration_date"] = date(2099, 12, 31)
        store_dim["is_current_row"] = "Y"

        # apply common transformation
        store_dim = self.dimension_transform(
            store_dim, store, store_location
        )

        # conform output types
        store_dim = store_dim.astype(
            self._dimension_table.get_column_pandas_types()
        )

        return store_dim

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
        :param product: staged customer data
        :param product_supplier: staged customer address data
        :return: a customer_dim dataframe ready to be written to mySQL
        """

        if prior_product_dim.shape[0] == 0:
            return pd.DataFrame([])

        # restrict stage date to update_keys
        product = product.loc[update_keys]
        product_supplier = product_supplier.loc[update_keys]

        update_dim = ProductDimensionProcessor.dimension_transform(
            prior_product_dim, product, product_supplier
        )

        # conform output types
        update_dim = update_dim.astype(self._dimension_table.get_column_pandas_types())

        return update_dim
