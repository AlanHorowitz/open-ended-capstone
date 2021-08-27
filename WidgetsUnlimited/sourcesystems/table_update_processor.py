from util.sqltypes import Table
from .generator import DataGenerator
from .base import BaseSystem
from .table_update import TableUpdate

class TableUpdateProcessor():
    
    def process(self, batch_id : int, table_update: TableUpdate) -> None:
        table = table_update.table
        op_system : BaseSystem = table.getOperationalSystem()
        generator : DataGenerator = op_system.getDataGenerator()
        i_rows, u_rows  = generator.generate(table_update, batch_id)
        op_system.insert(table, i_rows)
        op_system.update(table, u_rows)
