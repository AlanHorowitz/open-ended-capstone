from tables.product import ProductTable
from tables.customer import CustomerTable
from tables.customer_address import CustomerAddressTable
from tables.order import OrderTable
from tables.order_line_item import OrderLineItemTable

from warehouse.data_warehouse import DataWarehouse

from sourcesystems.generator import DataGenerator 
from sourcesystems.ecommerce import eCommerceSystem
from sourcesystems.inventory import InventorySystem
from sourcesystems.table_update import TableUpdate
from sourcesystems.table_update_processor import TableUpdateProcessor


""" Driver to load the source systems of Widgets Unlimited with sample data over a multi-day peridd.
    This data will be exposed in various formats as incremental updates to the data warehouse.

    phase #1 is confined to customers and orders in the e-commerce system
"""    

product_table = ProductTable()
customer_table = CustomerTable()
customer_address_table = CustomerAddressTable()
order_table = OrderTable()
order_line_item_table = OrderLineItemTable()

data_generator = DataGenerator()
warehouse = DataWarehouse()

e_commerce_system = eCommerceSystem(data_generator)
inventory_system = InventorySystem(data_generator)

# allocate the tables to their operational system.
e_commerce_system.add_tables([customer_table,
customer_address_table, order_table, order_line_item_table])
inventory_system.add_tables([product_table])

daily_operations = [
    [   
        # day 1  
        TableUpdate(product_table, n_inserts=500, n_updates=0), 
        TableUpdate(customer_table, n_inserts=200, n_updates=0),
        TableUpdate(customer_address_table, n_inserts=1, n_updates=0, link_parent=True),

    ],
    [
        # day 2
        TableUpdate(product_table, n_inserts=50, n_updates=0),
        TableUpdate(customer_table, n_inserts=40, n_updates=0),
        TableUpdate(customer_address_table, n_inserts=1, n_updates=0, link_parent=True),
        TableUpdate(order_table, n_inserts=1000, n_updates=0),
        TableUpdate(order_line_item_table, n_inserts=5, n_updates=0, link_parent=True),
        
    ],
    [
        # day 3
        TableUpdate(product_table, n_inserts=50, n_updates=0),
        TableUpdate(customer_table, n_inserts=0, n_updates=5),
        TableUpdate(customer_address_table, n_inserts=0, n_updates=10),
        TableUpdate(order_table, n_inserts=1000, n_updates=0),
        TableUpdate(order_line_item_table, n_inserts=3, n_updates=0, link_parent=True),           ],
]

table_update_processor = TableUpdateProcessor()

for day, updates in enumerate(daily_operations, start=1):
    for table_update in updates:
        table_update_processor.process(table_update=table_update, batch_id=day)

    warehouse.direct_extract(data_generator.get_connection(), batch_id=day) # get pg from env
    warehouse.transform_load(batch_id=day)

print("Phase 1 demo completed sucessfully.")
