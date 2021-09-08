from typing import List
from datetime import datetime

from model.metadata import Table


class BaseSystem:
    def __init__(self) -> None:
        pass

    def add_tables(self, tables: List[Table]) -> None:
        pass

    def insert(self, table, records):
        pass

    def update(self, table, records):
        pass
