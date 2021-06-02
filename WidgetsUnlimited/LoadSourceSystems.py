from sourcesystems.ecommerce import eCommerceSystem
from sourcesystems.basesystem import TableUpdate
from DataGeneration.Generator import Generator 

from domain.product import ProductTable
from domain.store import StoreTable
from domain.store_sales import STORE_SALES_TABLE

product_table = ProductTable()
store_table = StoreTable()

DailyOperations = [
        [   
            # day 1
            TableUpdate(product_table, 5000, 0), 
            TableUpdate(store_table, 40, 0)
        ],
        [
            # day 2
            TableUpdate(product_table, 5, 50),
            TableUpdate(store_table, 10, 20),
            TableUpdate(STORE_SALES_TABLE, 50000, 0),
        ],
        [
            # day 3
            TableUpdate(product_table, 10, 30),
            TableUpdate(store_table, 1, 0),
            TableUpdate(STORE_SALES_TABLE, 50000, 0),
        ],
    ]

data_generator = Generator()
eCommerceSys = eCommerceSystem(data_generator) 
eCommerceSys.add_tables([PRODUCT_TABLE, STORE_TABLE, STORE_SALES_TABLE])

# date = INIT_DATE
for day in DailyOperations:
    for table_update in day:
        table_update.process()
        # bump time delta one dat
    

# gen.close()
# ## what is my ETL going to do?

# eCommerceOpSystem.remove_tables([table1, table2]) 
# inStoreOpSystem.remove_tables([table1, table2])
# inventoryOpSystem.remove_tables([table1, table2])


