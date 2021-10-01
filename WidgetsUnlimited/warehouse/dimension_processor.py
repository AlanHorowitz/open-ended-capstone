class DimensionProcessor:
    def __init__(self):
        pass

    def _create_dimension(self):
        """Create an empty product_dimension on warehouse initialization."""

        cur = self._connection.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {self._dimension_table.get_name()};")
        cur.execute(self._dimension_table.get_create_sql_mysql())