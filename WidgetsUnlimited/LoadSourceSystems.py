from sourcesystems.ecommerce import eCommerceSystem
from sourcesystems.basesystem import TableUpdate
from DataGeneration.Generator import Generator 

from domain.product import ProductTable
from domain.store import StoreTable
from domain.store_sales import StoreSalesTable

product_table = ProductTable()
store_table = StoreTable()
store_sales_table = StoreSalesTable()

data_generator = Generator()
e_commerce_system = eCommerceSystem(data_generator) 
e_commerce_system.add_tables([product_table, store_table, store_sales_table])

DailyOperations = [
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

# date = INIT_DATE
for day in DailyOperations:
    for table_update in day:
        table_update.process()
        # bump time delta one dat
    
e_commerce_system.remove_tables([product_table, store_table, store_sales_table])
e_commerce_system.close()

data_generator.close()
