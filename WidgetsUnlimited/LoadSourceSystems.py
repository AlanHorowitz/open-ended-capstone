from collections import namedtuple
from util.sqltypes import Table
from SourceSystem.ECommerceSystem import eCommerceOperationalSystem
from DataGeneration.Generator import Generator, GeneratorItem 

from domain.product import PRODUCT_TABLE
from domain.store import STORE_TABLE
from domain.store_sales import STORE_SALES_TABLE

data_generator = Generator()

eCommerceOpSystem = eCommerceOperationalSystem() 

# Associate tables with operational systems;  
eCommerceOpSystem.add_tables([PRODUCT_TABLE, STORE_TABLE, STORE_SALES_TABLE])
data_generator.add_tables([PRODUCT_TABLE, STORE_TABLE, STORE_SALES_TABLE])

DailyOperations = [
        [   
            GeneratorItem(PRODUCT_TABLE, 5000, 0), 
            GeneratorItem(STORE_TABLE, 40, 0)
        ],
        [
            GeneratorItem(PRODUCT_TABLE, 5, 50),
            GeneratorItem(STORE_TABLE, 10, 20),
            GeneratorItem(STORE_SALES_TABLE, 50000, 0),
        ],
        [
            GeneratorItem(PRODUCT_TABLE, 10, 30),
            GeneratorItem(STORE_TABLE, 1, 0),
            GeneratorItem(STORE_SALES_TABLE, 50000, 0),
        ],
    ]

for updates in DailyOperations:
    data_generator.generate_and_add(updates)
    

# gen.close()
# ## what is my ETL going to do?

# eCommerceOpSystem.remove_tables([table1, table2]) 
# inStoreOpSystem.remove_tables([table1, table2])
# inventoryOpSystem.remove_tables([table1, table2])


