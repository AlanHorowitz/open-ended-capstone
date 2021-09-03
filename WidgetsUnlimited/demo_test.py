""" 
Widgets Unlimited - A data warehousing simulation.

demo1.py - Execution of Widgets Unlimited's operations over 3 days. Data Generation
and transformation of customer dimension.

Core classes:

Tables - Metadata describing data elements.
Source Systems -
DataGenerator - 
SourceSystemController - Processes sets of TableTransactions, 
by generating the 
daily data and feeding it to the appropriate source system
DataWarehouse - supports extraction of incremental data produced by the source system.

Demo 1 generates 3 days of data for the e-commerce system, which is modelled by  
customer, customer_address, order and order_line item tables. As orders depend upon products,  
product records are generated as well.

In the final project, the e-commerce system will expose incremental changes to the system via a 
kafka topic which will be ingested via the warehouse.  In this dermo
"""

from tables.product import ProductTable
from tables.customer import CustomerTable
from tables.customer_address import CustomerAddressTable
from tables.order import OrderTable
from tables.order_line_item import OrderLineItemTable

from sourcesystems.ecommerce import eCommerceSystem
from sourcesystems.inventory import InventorySystem

from sourcesystems.generator import DataGenerator

from sourcesystems.table_transaction import TableTransaction
from sourcesystems.controller import SourceSystemController

from warehouse.data_warehouse import DataWarehouse


# table metadata (portions of the data model)
product_table = ProductTable()
customer_table = CustomerTable()
customer_address_table = CustomerAddressTable()
order_table = OrderTable()
order_line_item_table = OrderLineItemTable()

# creates sample records for a table
data_generator = DataGenerator()

# source systems
e_commerce_system = eCommerceSystem(data_generator)
inventory_system = InventorySystem(data_generator)

# allocate tables to source systems
e_commerce_system.add_tables(
    [customer_table, customer_address_table, order_table, order_line_item_table]
)
inventory_system.add_tables([product_table])

# processor of source system inputs
source_system_controller = SourceSystemController()

# consumes and transforms source system incremental output
warehouse = DataWarehouse()

# three days sample of input
daily_operations = (
    [
        # day 1
        TableTransaction(product_table, n_inserts=500, n_updates=0),
        TableTransaction(customer_table, n_inserts=200, n_updates=0),
        TableTransaction(
            customer_address_table, n_inserts=1, n_updates=0, link_parent=True
        ),
    ],
    [
        # day 2
        TableTransaction(product_table, n_inserts=50, n_updates=0),
        TableTransaction(customer_table, n_inserts=40, n_updates=0),
        TableTransaction(
            customer_address_table, n_inserts=0, n_updates=5, link_parent=False
        ),
        # TableTransaction(order_table, n_inserts=1000, n_updates=0),
        # TableTransaction(
        #     order_line_item_table, n_inserts=5, n_updates=0, link_parent=True
        # ),
    ],
    [
        # day 3
        TableTransaction(product_table, n_inserts=50, n_updates=5),
        TableTransaction(customer_table, n_inserts=500, n_updates=5),
        TableTransaction(customer_address_table, n_inserts=1, n_updates=0, link_parent=True),
    ],
    [
        # day 4
        TableTransaction(customer_address_table, n_inserts=0, n_updates=15, link_parent=False),
    ],
)

# Each day, synchronously process transactions and extract to
# warehouse.
for day, transactions in enumerate(daily_operations, start=1):

    print("-"*60)
    print(f"Batch {day} ")
    print("-"*60)
    source_system_controller.process(transactions=transactions, batch_id=day)
    warehouse.direct_extract(data_generator.get_connection(), batch_id=day)
    warehouse.transform_load(batch_id=day)

print("Phase 1 demo completed successfully.")
