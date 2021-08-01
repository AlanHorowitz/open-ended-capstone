from typing import List
from datetime import datetime

from util.sqltypes import Table
from .generator import DataGenerator

class BaseSystem:
    def __init__(self, dataGenerator : DataGenerator) -> None:
        self._data_generator = dataGenerator

    def add_tables(self, tables : List[Table]) -> None:
        self._data_generator.add_tables(tables)
        for table in tables:
            table.setOperationalSystem(self)

    def open(table : Table) -> None:
        pass

    def close(table):
        pass

    def insert(self, table, records):
        pass

    def update(self, table, records):
        pass

    def getDataGenerator(self) -> DataGenerator:
        return self._data_generator

