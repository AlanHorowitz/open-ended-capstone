from typing import List
from datetime import datetime

from util.sqltypes import Table, Column
from datageneration.datagenerator import DataGenerator

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

class TableUpdate():
    def __init__(self, table : Table, n_inserts : int, n_updates : int) -> None:
        self.table = table
        self.n_inserts = n_inserts
        self.n_updates = n_updates

    def process(self) -> None:
        op_system : BaseSystem = self.table.getOperationalSystem()
        generator : DataGenerator = op_system.getDataGenerator()
        i_rows, u_rows  = generator.generate(self.table, self.n_inserts, self.n_updates, datetime.now())
        op_system.insert(self.table, i_rows)
        op_system.update(self.table, u_rows)
