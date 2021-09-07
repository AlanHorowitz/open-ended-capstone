from model.metadata import Table
from .generator import DataGenerator, GeneratorRequest
from .base import BaseSystem
from typing import List


class OperationsSimulator:

    def __init__(self, data_generator: DataGenerator, source_systems: List[BaseSystem]):

        self._data_generator = data_generator
        self._source_systems = set(source_systems)
        self._source_system_lookup = {}

    def add_tables(self, source_system: BaseSystem, tables: List[Table]) -> None:
        """
        Associate a list of Tables to a source system and pass the tables to both source
        system and data generator for initialization.

        Create a dictionary mapping table names to the source system objects.  A table may only
        be associated with a single source system.

        :param source_system: source system object
        :param tables: list of Table objects
        :return: None, raises an exception if source system is unknown or table is added more than once
        """
        if source_system not in self._source_systems:
            raise Exception("Error. May not add tables to unknown source system")
        for table in tables:
            table_name = table.get_name()
            if table_name in self._source_system_lookup:
                raise Exception("Error.  Table may only be added once to simulator")
            self._source_system_lookup[table_name] = source_system
        source_system.add_tables(tables)
        self._data_generator.add_tables(tables)

    def process(self, batch_id: int, transactions: List[GeneratorRequest]) -> None:
        """
        Feed a list of transactions to the data generator
        :param batch_id:
        :param transactions:
        :return:
        """

        for transaction in transactions:
            table = transaction.table
            op_system: BaseSystem = self._source_system_lookup[table.get_name()]
            i_rows, u_rows = self._data_generator.generate(transaction, batch_id)
            op_system.insert(table, i_rows)
            op_system.update(table, u_rows)
