from pandas.core.frame import DataFrame


class DimensionProcessor:
    def __init__(self, connection, dim_table):
        self._connection = connection
        self._dimension_table = dim_table
        if connection:
            self._create_dimension()

    def _create_dimension(self):
        """Create an empty dimension table on warehouse initialization."""

        table_name = self._dimension_table.get_name()
        cur = self._connection.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        cur.execute(self._dimension_table.get_create_sql_mysql())
        print(
            f"{self.__class__.__name__}: {table_name} table created"
        )

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
                f"{self.__class__.__name__}: {rows_affected} {operation_text} written to {table_name} table"
            )

            self._connection.commit()
