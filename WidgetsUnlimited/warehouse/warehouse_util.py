import os
import logging
from typing import List

import pandas as pd
from WidgetsUnlimited.model.metadata import Table

STAGE_DIRECTORY_PREFIX = "/tmp/warehouse/stage/batch"
logger = logging.getLogger(__name__)


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
        stage_file = get_stage_file(batch_id, table_name)
        if os.path.exists(stage_file):
            df = pd.read_parquet(stage_file)
            df = df.astype(table.get_column_pandas_types())
        else:
            df = pd.DataFrame([], columns=table.get_column_names())
        logger.debug(
            f"ReadStage: {df.shape[0]} {table_name} records read from stage."
        )
        df = df.set_index(index_column, drop=False)
        stages.append(df)

    return stages


def extract_write_stage(
    connection, batch_id: int, tables: List[Table], cumulative: bool = False
) -> None:
    """
    Read from data generator and write to stage parquet file for each table in batch
    :param cumulative: If true, do not filter on batch_id
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

    for table in tables:
        table_name = table.get_name()
        column_names = ",".join(table.get_column_names())

        where = "" if cumulative else f"WHERE batch_id = {batch_id}"
        sql = f"SELECT {column_names} from {table_name} {where};"
        cur = connection.cursor()
        cur.execute(sql)
        result = cur.fetchall()

        df = pd.DataFrame(result, columns=table.get_column_names())
        df_type = {
            col.get_name(): pd_types[col.get_type()] for col in table.get_columns()
        }
        df = df.astype(df_type)
        df.to_parquet(get_stage_file(batch_id, table_name), compression="gzip")
        logger.debug(f"direct-extract: {df.shape[0]} {table_name} records extracted to stage")
