from collections import namedtuple
from util.sqltypes import Table
# from generator import InStoreOperationalSystem
from generator.ECommerceOperationalSystem import eCommerceOperationalSystem
# from generator import ProductOperationalSystem
from generator.Generator import Generator, GeneratorItem 

from tables.product import PRODUCT_TABLE
from tables.store import STORE_TABLE
from tables.store_sales import STORE_SALES_TABLE

gen = Generator()

eCommerceOpSystem = eCommerceOperationalSystem() 
# inStoreOpSystem = InStoreOperationalSystem() 
# productOpSystem = ProductOperationalSystem()

# # Associate tables with operational systems;  
eCommerceOpSystem.add_tables([PRODUCT_TABLE, STORE_TABLE])
gen.add_tables([PRODUCT_TABLE, STORE_TABLE])
# # inStoreOpSystem.add_tables([table1, table2])
# # productOpSystem.add_tables([table1, table2])

DailyOperations = [
        [   
            GeneratorItem(PRODUCT_TABLE, 5000, 0), 
            GeneratorItem(STORE_TABLE, 40, 0)
        ],
        [
            GeneratorItem(PRODUCT_TABLE, 5, 50),
            GeneratorItem(STORE_TABLE, 10, 20),
        #     GeneratorItem(STORE_SALES_TABLE, 50000, 0),
        # ],
        # [
        #     GeneratorItem(PRODUCT_TABLE, 10, 30),
        #     GeneratorItem(STORE_TABLE, 1, 0),
        #     GeneratorItem(STORE_SALES_TABLE, 50000, 0),
        ],
    ]

for updates in DailyOperations:
    gen.run(updates)
    # ping ETL service to do daily update
    # work on CSV coordination later
    #  get user input for next day will allow me to test incremental before rest service 

# gen.close()
# ## what is my ETL going to do?

# eCommerceOpSystem.remove_tables([table1, table2]) 
# inStoreOpSystem.remove_tables([table1, table2])
# inventoryOpSystem.remove_tables([table1, table2])


