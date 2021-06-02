from util.sqltypes import Table, Column
from typing import List
from DataGeneration.Generator

class BaseSystem:
    def __init__(self, dataGenerator : Generator) -> None:
        self._data_generator = dataGenerator

    def add_tables(self, tables : List[Table]) -> None:
        self.data_generator.add_tables(tables)
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

    def getDataGenerator(self) -> 


class TableUpdate():
    def __init__(self, table : Table, n_inserts : int, n_updates : int) -> None:
        self.table = table
        self.n_inserts = n_inserts
        self.n_updates = n_updates

    def process(self) -> None:
        opSystem : OperationalSourceSystem = table.getOperationalSystem()
        generator : DataGenerator = opSystem.getDataGenerator()
        i_rows, u_rows  = generator.generate(self.table, self.n_inserts, self.n_updates, datetime.now())
        opSystem.insert(self.table, i_rows)
        opSystem.update(self.table, u_rows)
