from typing import List
from datetime import datetime

from util.sqltypes import Table


class BaseSystem:
    def __init__(self) -> None:
        pass

    def add_tables(self, tables: List[Table]) -> None:
        pass

    def open(table: Table) -> None:
        pass

    def close(table):
        pass

    def insert(self, table, records):
        pass

    def update(self, table, records):
        pass
