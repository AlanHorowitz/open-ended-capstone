from sourcesystems.ecommerce import eCommerceSystem
from sourcesystems.basesystem import TableUpdate
from datageneration.datagenerator import DataGenerator 

from tables.product import ProductTable
from tables.store import StoreTable
from tables.store_sales import StoreSalesTable

product_table = ProductTable()
store_table = StoreTable()
store_sales_table = StoreSalesTable()

data_generator = DataGenerator()
e_commerce_system = eCommerceSystem(data_generator) 
e_commerce_system.add_tables([product_table, store_table, store_sales_table])

daily_operations = [
    [   
        # day 1
        TableUpdate(product_table, n_inserts=5000, n_updates=0), 
        TableUpdate(store_table, n_inserts=40, n_updates=0)
    ],
    [
        # day 2
        TableUpdate(product_table, n_inserts=5, n_updates=50),
        TableUpdate(store_table, n_inserts=10, n_updates=20),
        TableUpdate(store_sales_table, n_inserts=50000, n_updates=0),
    ],
    [
        # day 3
        TableUpdate(product_table, n_inserts=10, n_updates=30),
        TableUpdate(store_table, n_inserts=1, n_updates=0),
        TableUpdate(store_sales_table, n_inserts=50000, n_updates=0),
    ],
]

for day in daily_operations:
    for table_update in day:
        table_update.process()
        