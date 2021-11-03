from WidgetsUnlimited.model.metadata import Table
from typing import List
from .base import BaseSystem


class InStoreSystem(BaseSystem):
    def __init__(self) -> None:
        super().__init__()

    def add_tables(self, tables: List[Table]) -> None:
        # create all the kafka topics
        pass

    def insert(self, table, records):
        pass

    def update(self, table, records):
        pass
