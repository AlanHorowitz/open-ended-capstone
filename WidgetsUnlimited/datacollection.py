# Open-ended Capstone Step 3: Collect Your Data

# The WidgetsUnlimited project synthesizes its own source data instead of acquiring it from a 
# third party. This synthesis is here demonstrated as the "collection phase."

# Features of Data Generation

# - Three domains are currently modeled: Product, Store and Store Sales.
# - Monotonically increasing primary keys
# - Hard-coded values (of the appropriate type) are used for the data columns. This will be replaced by randomized 
#   ranges including some "dirty data" later in the project.
# - Real foreign key references are used for product and store in store sales.
# - Generated data us writen to postgres.

# See sourcesystems.generator.py and util.sqltypes.py for implemetation details.

from datetime import datetime

from sourcesystems.generator import DataGenerator

from tables.product import ProductTable
from tables.store import StoreTable
from tables.store_sales import StoreSalesTable

data_generator = DataGenerator()

product_table = ProductTable()
store_table = StoreTable()
store_sales_table = StoreSalesTable()

data_generator.add_tables([product_table, store_table, store_sales_table])

# Initialize with 100 stores, 1000 products and 50000 sales transactions.
data_generator.generate(product_table, n_inserts=1000, n_updates=0, timestamp=datetime.now())
data_generator.generate(store_table, n_inserts=100, n_updates=0, timestamp=datetime.now())
data_generator.generate(store_sales_table, n_inserts=50000, n_updates=0, timestamp=datetime.now())

# Add/Update products and stores. Add new sales transactions.
data_generator.generate(product_table, n_inserts=10, n_updates=50, timestamp=datetime.now())
data_generator.generate(store_table, n_inserts=1, n_updates=20, timestamp=datetime.now())
data_generator.generate(store_sales_table, n_inserts=50000, n_updates=0, timestamp=datetime.now())