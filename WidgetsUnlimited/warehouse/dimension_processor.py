from pandas.core.frame import DataFrame
import logging

logger = logging.getLogger(__name__)


class DimensionProcessor:
    def __init__(self, connection, dim_table):
        self._connection = connection
        self._dimension_table = dim_table
        if connection:
            self._create(self._dimension_table)

    def _create(self, table):
        """Create an empty mySQL table on warehouse initialization."""

        table_name = table.get_name()
        cur = self._connection.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        cur.execute(table.get_create_sql_mysql())

        logger.debug(f"{table_name} table created")

        self._connection.commit()

    def _write_dimension(self, dim_table: DataFrame, operation: str) -> None:
        """
        Write a dataframe containing inserts or updates to mySQL dimension table.
        Convert the dataframe to a python list and use mysql-connector-python for bulk execution call.

        :param dim_table: dataframe conforming to customer_dim schema
        :param operation: INSERT/REPLACE -- mirror mySQL verbs for insert/upsert
        :return: None
        """
        if dim_table.shape[0] > 0:
            table = self._dimension_table
            table_name = table.get_name()
            column_names = ",".join(table.get_column_names())
            values_substitutions = ",".join(["%s"] * len(table.get_column_names()))
            cur = self._connection.cursor()
            rows = dim_table.to_numpy().tolist()

            cur.executemany(
                f"{operation} INTO {table_name} ({column_names}) values ({values_substitutions})",
                rows,
            )

            if operation == "INSERT":
                operation_text = "inserts"
                rows_affected = cur.rowcount
            else:
                operation_text = "updates"
                rows_affected = cur.rowcount // 2  # REPLACE implemented as DELETE then INSERT
            logger.debug(
                f"{rows_affected} {operation_text} written to {table_name} table"
            )

            self._connection.commit()
