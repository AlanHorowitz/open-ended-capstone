from .util import clean_stage_dir, get_stage_dir
from typing import List
from util.sqltypes import Column, Table
from tables.customer import CustomerTable
from tables.customer_address import CustomerAddressTable
from .customer_dimension import CustomerDimension
import os
import pandas as pd
from mysql.connector import connect


class DataWarehouse:
    def __init__(self) -> None:
        self.ms_connection = connect(
            host=os.getenv("WAREHOUSE_HOST"),
            port=os.getenv("WAREHOUSE_PORT"),
            user=os.getenv("WAREHOUSE_USER"),
            password=os.getenv("WAREHOUSE_PASSWORD"),
            database=os.getenv("WAREHOUSE_DB"),
            charset="utf8",
        )

        self.customer_dimesion = CustomerDimension(self.ms_connection)

    def direct_extract(self, connection, batch_id):
        self.write_parquet_warehouse_tables(
            connection, batch_id, [CustomerTable(), CustomerAddressTable()]
        )

    def transform_load(self, batch_id):
        self.customer_dimesion.process_update(batch_id=batch_id)

    def write_parquet_warehouse_tables(
        self, connection, batch_id: int, tables: List[Table]
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
