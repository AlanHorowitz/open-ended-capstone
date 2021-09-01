from .util import clean_stage_dir, get_stage_dir
from typing import List
from util.sqltypes import Column, Table
from tables.customer import CustomerTable
from tables.customer_address import CustomerAddressTable
from .customer_dimension import CustomerDimensionProcessor
import os
import pandas as pd
from mysql.connector import connect


class DataWarehouse:
    """
    The Data Warehouse ingests and processes incremental batches of transactions from multiple source
    systems.  Data from the source systems are persisted to a staging area as parquet files.  What all the staging data
    have been written for a batch, a series of transformations are launched which updates a star schema in mySQL.
    """
    def __init__(self) -> None:
        """
        Connect to mySQL for star schema and initialize transformation classes
        """
        self._ms_connection = connect(
            host=os.getenv("WAREHOUSE_HOST"),
            port=os.getenv("WAREHOUSE_PORT"),
            user=os.getenv("WAREHOUSE_USER"),
            password=os.getenv("WAREHOUSE_PASSWORD"),
            database=os.getenv("WAREHOUSE_DB"),
            charset="utf8",
        )

        # phase 1 - single transformation
        self._customer_dimension = CustomerDimensionProcessor(self._ms_connection)

    def direct_extract(self, connection, batch_id):
        """
        Extract incremental updates directly from the data generator database and write to staging area

        This is a shortcut employed for testing and demo purposes. It substitutes for two steps the completed
        version of the WidgetsUnlimited project: 1) Source system specific exposure of incremental updates by the
        OperationsSimulator; 2) Source system specific ingestion of incremental updates by the DataWarehouse.

        The input tables for the customer dimension are hard coded in phase #1

        :param connection: connection to data generator database
        :param batch_id: identifier of incremental batch
        :return: None
        """
        self.write_parquet_warehouse_tables(
            connection, batch_id, [CustomerTable(), CustomerAddressTable()]
        )

    def transform_load(self, batch_id):
        """
        Transform input from staging area into updated mySQL star schema.

        :param batch_id: identifier of incremental batch
        :return: None
        """
        # phase 1 - single transformation
        self._customer_dimension.process_update(batch_id=batch_id)

    @staticmethod
    def write_parquet_warehouse_tables(
            connection, batch_id: int, tables: List[Table]
    ):

        pd_types = {
            "INTEGER": "int64",
            "VARCHAR": "string",
            "FLOAT": "float64",
            "DATE": "datetime64[ns]",
            "BOOLEAN": "bool",
            "TIMESTAMP": "datetime64[ns]",
        }

        clean_stage_dir(batch_id)

        for table in tables:
            table_name = table.get_name()
            column_names = ",".join(table.get_column_names())

            sql = (
                f"SELECT {column_names} from {table_name} WHERE batch_id = {batch_id};"
            )

            cur = connection.cursor()
            cur.execute(sql)

            result = cur.fetchall()

            df = pd.DataFrame(result, columns=table.get_column_names())

            dtype1 = {
                col.get_name(): pd_types[col.get_type()] for col in table.get_columns()
            }
            df = df.astype(dtype1)
            df.to_parquet(
                os.path.join(get_stage_dir(batch_id), f"{table_name}.pr"),
                compression="gzip",
            )
