from .warehouse_util import extract_write_stage
from model.customer import CustomerTable
from model.customer_address import CustomerAddressTable
from .customer_dimension import CustomerDimensionProcessor
import os
import pandas as pd
from mysql.connector import connect


class DataWarehouse:
    """
    The Data Warehouse ingests and processes incremental batches of generator_requests from multiple source
    systems.  Data from the source systems are persisted to a staging area as parquet files.  When all the staging data
    have been written for a batch, a series of transformations are launched which update a star schema in mySQL.
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

    @staticmethod
    def direct_extract(connection, batch_id):
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
        extract_write_stage(
            connection, batch_id, [CustomerTable(), CustomerAddressTable()]
        )

    def transform_load(self, batch_id):
        """
        Transform inputs from staging area into updated mySQL star schema.

        :param batch_id: identifier of incremental batch
        :return: None
        """
        # phase 1 - single transformation
        self._customer_dimension.process_update(batch_id=batch_id)
