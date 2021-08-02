from sourcesystems.generator import DataGenerator
from sourcesystems.table_update import TableUpdate
from tables.order import OrderTable
from tables.order_line_item import OrderLineItemTable

import os

# test docker image
os.environ['DATA_GENERATOR_DB'] = 'postgres'
os.environ['DATA_GENERATOR_HOST'] = '172.17.0.2'
os.environ['DATA_GENERATOR_PORT'] = '5432'
os.environ['DATA_GENERATOR_USER'] = 'postgres'
os.environ['DATA_GENERATOR_PASSWORD'] = 'postgres'
os.environ['DATA_GENERATOR_SCHEMA'] = 'test'

generator = DataGenerator()
order_table = OrderTable()
order_line_item_table = OrderLineItemTable()

generator.add_tables([order_table, order_line_item_table])

generator.generate(TableUpdate(order_table, n_inserts=10, n_updates=0, batch_id=1))
generator.generate(TableUpdate(order_line_item_table, n_inserts=3, n_updates=0, batch_id=1, link_parent=True))


