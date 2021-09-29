from typing import Tuple, Dict
import pandas as pd
from datetime import date

from pandas.core.frame import DataFrame, Series, Index
from model.date_dim import DateDimTable




class DateDimensionProcessor:
    """ Initialize a static date dimension """

    def __init__(self, connection=None):
        """
        Initialize CustomerDimensionProcessor
        :param connection: mySQL connection created by the warehouse.  None is used for test.
        """

        self._connection = connection
        self._dimension_table = DateDimTable()
        self._next_surrogate_key = 1
        if connection:
            self._create_dimension()

    def build_dimension(self, start_date: date, end_date: date):

        date_dim = self._build_dimension(start_date, end_date)
        self._write_dimension(date_dim, "INSERT")
        pass

    def _build_dimension(self, start_date: date, end_date: date):
        return self._dimension_table

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
        query = f"SELECT {columns_list}, {key_name} FROM {table_name};"
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
