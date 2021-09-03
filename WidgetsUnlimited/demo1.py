""" 
Widgets Unlimited - A data warehousing simulation.

demo1.py - Execute Widgets Unlimited's operations over a period of 4 days. Data generation is restricted to tables
involving products, customers and orders.  Customer data are transformed to the format of a customer dimension in a
star schema.

Core classes:

SourceSystem - One of three logical systems that capture the business operations
Table - Metadata describing a data structures used by a source systems.
DataGenerator - Synthesizer of sample records for a table.
TableTransaction - An instruction to the data generator
OperationsSimulator - Process that feeds TableTransactions to the DataGenerator and appropriate source systems.
DataWarehouse - Process that extracts incremental data produced by the source systems and transforms it into a
star schema.
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
from sourcesystems.controller import OperationsSimulator
from warehouse.data_warehouse import DataWarehouse

# table metadata
PRODUCT = ProductTable()
CUSTOMER = CustomerTable()
CUSTOMER_ADDRESS = CustomerAddressTable()
ORDER = OrderTable()
ORDER_LINE_ITEM = OrderLineItemTable()

# creates sample records for a table
data_generator = DataGenerator()

# source systems
e_commerce_system = eCommerceSystem(data_generator)
inventory_system = InventorySystem(data_generator)

# allocate tables to source systems
e_commerce_system.add_tables(
    [CUSTOMER, CUSTOMER_ADDRESS, ORDER, ORDER_LINE_ITEM]
)
inventory_system.add_tables([PRODUCT])

# processor of source system inputs
operations_simulator = OperationsSimulator()

# consumes and transforms source system incremental output
warehouse = DataWarehouse()

# three days sample of input
daily_operations = [
    [
        # day 1
        TableTransaction(PRODUCT, n_inserts=500, n_updates=0),
        TableTransaction(CUSTOMER, n_inserts=200, n_updates=0),
        TableTransaction(
            CUSTOMER_ADDRESS, n_inserts=1, n_updates=0, link_parent=True
        ),
    ],
    [
        # day 2
        TableTransaction(PRODUCT, n_inserts=50, n_updates=0),
        TableTransaction(CUSTOMER, n_inserts=40, n_updates=0),
        TableTransaction(
            CUSTOMER_ADDRESS, n_inserts=1, n_updates=0, link_parent=True
        ),
        TableTransaction(ORDER, n_inserts=1000, n_updates=0),
        TableTransaction(
            ORDER_LINE_ITEM, n_inserts=5, n_updates=0, link_parent=True
        ),
    ],
    [
        # day 3
        TableTransaction(PRODUCT, n_inserts=50, n_updates=5),
        TableTransaction(CUSTOMER, n_inserts=0, n_updates=5),
        TableTransaction(CUSTOMER_ADDRESS, n_inserts=0, n_updates=10),
        TableTransaction(ORDER, n_inserts=1000, n_updates=0),
        TableTransaction(
            ORDER_LINE_ITEM, n_inserts=3, n_updates=0, link_parent=True
        ),
    ],
    [
        # day 4
        TableTransaction(CUSTOMER, n_inserts=25, n_updates=7),
        TableTransaction(
            CUSTOMER_ADDRESS, n_inserts=1, n_updates=0, link_parent=True
        ),
    ],
]

# Each day, synchronously process transactions and extract to the warehouse.
for day, transactions in enumerate(daily_operations, start=1):

    print("-"*60)
    print(f"Batch {day} starting")
    print("-"*60)
    operations_simulator.process(transactions=transactions, batch_id=day)
    warehouse.direct_extract(data_generator.get_connection(), batch_id=day)
    warehouse.transform_load(batch_id=day)

print("\ndemo1.py completed successfully.")
