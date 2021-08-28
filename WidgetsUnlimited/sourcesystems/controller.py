from util.sqltypes import Table
from .generator import DataGenerator
from .base import BaseSystem
from .table_transaction import TableTransaction


class SourceSystemController:

    def __init__(self, data_generator : DataGenerator) -> None:
        self._data_generator = data_generator

        
    def process(self, batch_id: int, transactions: TableTransaction) -> None:
        for transaction in transactions:

            table = transaction.table
            op_system: BaseSystem = table.getOperationalSystem()
            generator: DataGenerator = self._data_generator
            i_rows, u_rows = generator.generate(transaction, batch_id)
            op_system.insert(table, i_rows)
            op_system.update(table, u_rows)
