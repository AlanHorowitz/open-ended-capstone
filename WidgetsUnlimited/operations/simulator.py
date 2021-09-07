from tables.sqltypes import Table
from .generator import DataGenerator
from .base import BaseSystem
from .table_transaction import TableTransaction
from typing import List


class OperationsSimulator:
    def __init__(self, data_generator, source_systems):
        self._data_generator = data_generator
        self._source_systems = set(source_systems)
        self._source_system_lookup = {}

    def add_tables(self, source_system, tables):

        if source_system not in self._source_systems:
            raise Exception("Error. May not add tables to unknown source system")
        for table in tables:
            table_name = table.get_name()
            if table_name in self._source_system_lookup:
                raise Exception("Error.  Table may only be added once to simulator")
            self._source_system_lookup[table_name] = source_system
        source_system.add_tables(tables)
        self._data_generator.add_tables(tables)

    def process(self, batch_id: int, transactions: List[TableTransaction]) -> None:

        for transaction in transactions:
            table = transaction.table
            op_system: BaseSystem = self._source_system_lookup[table.get_name()]
            i_rows, u_rows = self._data_generator.generate(transaction, batch_id)
            op_system.insert(table, i_rows)
            op_system.update(table, u_rows)
