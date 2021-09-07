import os
from typing import List

import pandas as pd
from model.metadata import Column, Table

STAGE_DIRECTORY_PREFIX = "/tmp/warehouse/stage/batch"


def clean_stage_dir(batch_id):
    """Clean out stage directory for a batch or create if it doesn't"""
    out_path = STAGE_DIRECTORY_PREFIX + str(batch_id)
    if os.path.exists(out_path):
        for f in os.listdir(out_path):
            os.remove(os.path.join(out_path, f))
    else:
        os.makedirs(out_path)


def get_stage_file(batch_id, table_name) -> str:
    """Return the full path name of stage file for a batch and table name"""
    return os.path.join(STAGE_DIRECTORY_PREFIX + str(batch_id), f"{table_name}.parquet")


def read_stage(batch_id: int, tables) -> List[pd.DataFrame]:
    """
    Read stage files and instantiate dataframes with the primary key as index
    :param batch_id: identifier of incremental batch
    :param tables: table metadata for files
    :return: list of indexed dataframes, in order of tables argument
    """
    stages = []
    for table in tables:
        table_name = table.get_name()
        index_column = (
            table.get_parent_key() if table.has_parent() else table.get_primary_key()
        )
        df = pd.read_parquet(get_stage_file(batch_id, table_name))
        df = df.astype(table.get_column_pandas_types())
        df = df.set_index(index_column, drop=False)
        print(
            f"CustomerDimensionProcessor: {df.shape[0]} {table_name} records read from stage"
        )
        stages.append(df)

    return stages


def extract_write_stage(connection, batch_id: int, tables: List[Table]) -> None:
    """
    Read from data generator and write to stage parquet file for each table in batch
    :param connection: Connection to data generator
    :param batch_id: identifier of incremental batch
    :param tables: table metadata for files
    :return: None
    """

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

        sql = f"SELECT {column_names} from {table_name} WHERE batch_id = {batch_id};"
        cur = connection.cursor()
        cur.execute(sql)
        result = cur.fetchall()

        df = pd.DataFrame(result, columns=table.get_column_names())
        df_type = {
            col.get_name(): pd_types[col.get_type()] for col in table.get_columns()
        }
        df = df.astype(df_type)
        df.to_parquet(get_stage_file(batch_id, table_name), compression="gzip")
        print(f"direct-extract: {df.shape[0]} {table_name} records extracted to stage")
