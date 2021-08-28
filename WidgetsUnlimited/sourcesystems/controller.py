from util.sqltypes import Table
from .generator import DataGenerator
from .base import BaseSystem
from .table_transaction import TableTransaction


class SourceSystemController:
    def process(self, batch_id: int, transactions: TableTransaction) -> None:

        for transaction in transactions:

            table = transaction.table
            op_system: BaseSystem = table.getOperationalSystem()
            generator: DataGenerator = op_system.getDataGenerator()
            i_rows, u_rows = generator.generate(transaction, batch_id)
            op_system.insert(table, i_rows)
            op_system.update(table, u_rows)
