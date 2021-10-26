from WidgetsUnlimited.model.metadata import Table
from typing import List


class InStoreSystem:
    def __init__(self) -> None:
        # open connection to postgres
        pass

    def add_tables(self, tables: List[Table]) -> None:
        # create all the kafka topics
        pass

    def insert(self, table, records):
        pass

    def update(self, table, records):
        pass
