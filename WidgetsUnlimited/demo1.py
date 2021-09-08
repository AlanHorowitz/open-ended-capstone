""" 
Widgets Unlimited - A data warehousing simulation.

demo1.py - Execute Widgets Unlimited's operations over a period of 4 days. Data create_only is restricted to tables
involving products, customers and orders.  Customer data are transformed to the format of a customer dimension in a
star schema.

Core classes:

SourceSystem - One of three logical systems that capture the business operations
Table - Metadata describing a data structures used by a source systems.
DataGenerator - Synthesizer of sample records for a table.
GeneratorRequest - An instruction to the data generator
OperationsSimulator - Process that feeds GeneratorRequests to the DataGenerator and appropriate source systems.
DataWarehouse - Process that extracts incremental data produced by the source systems and transforms it into a
star schema.
"""

from model.product import ProductTable
from model.customer import CustomerTable
from model.customer_address import CustomerAddressTable
from model.order import OrderTable
from model.order_line_item import OrderLineItemTable

from operations.base import BaseSystem
from operations.ecommerce import eCommerceSystem
from operations.inventory import InventorySystem
from operations.generator import DataGenerator, GeneratorRequest
from operations.simulator import OperationsSimulator

from warehouse.data_warehouse import DataWarehouse

# table metadata
PRODUCT = ProductTable()
CUSTOMER = CustomerTable()
CUSTOMER_ADDRESS = CustomerAddressTable()
ORDER = OrderTable()
ORDER_LINE_ITEM = OrderLineItemTable()

# create data generator
data_generator = DataGenerator()

# create source systems
e_commerce_system: BaseSystem = eCommerceSystem()
inventory_system: BaseSystem = InventorySystem()

# Initialize simulator with data generator and source systems.  Allocate tables to source systems.
operations_simulator = OperationsSimulator(
    data_generator, [e_commerce_system, inventory_system]
)
operations_simulator.add_tables(
    e_commerce_system, [CUSTOMER, CUSTOMER_ADDRESS, ORDER, ORDER_LINE_ITEM]
)
operations_simulator.add_tables(inventory_system, [PRODUCT])

# create data warehouse
warehouse = DataWarehouse()

# four days of operations input
daily_operations = [
    [
        # day 1
        GeneratorRequest(PRODUCT, n_inserts=500, n_updates=0),
        GeneratorRequest(CUSTOMER, n_inserts=200, n_updates=0),
        GeneratorRequest(CUSTOMER_ADDRESS, n_inserts=1, n_updates=0, link_parent=True),
    ],
    [
        # day 2
        GeneratorRequest(PRODUCT, n_inserts=50, n_updates=0),
        GeneratorRequest(CUSTOMER, n_inserts=40, n_updates=0),
        GeneratorRequest(CUSTOMER_ADDRESS, n_inserts=1, n_updates=0, link_parent=True),
        GeneratorRequest(ORDER, n_inserts=1000, n_updates=0),
        GeneratorRequest(ORDER_LINE_ITEM, n_inserts=5, n_updates=0, link_parent=True),
    ],
    [
        # day 3
        GeneratorRequest(PRODUCT, n_inserts=50, n_updates=5),
        GeneratorRequest(CUSTOMER, n_inserts=0, n_updates=5),
        GeneratorRequest(CUSTOMER_ADDRESS, n_inserts=0, n_updates=10),
        GeneratorRequest(ORDER, n_inserts=1000, n_updates=0),
        GeneratorRequest(ORDER_LINE_ITEM, n_inserts=3, n_updates=0, link_parent=True),
    ],
    [
        # day 4
        GeneratorRequest(CUSTOMER, n_inserts=25, n_updates=61),
        GeneratorRequest(CUSTOMER_ADDRESS, n_inserts=1, n_updates=0, link_parent=True),
    ],
]

# Synchronously process transactions and extract and load in warehouse.
for day, transactions in enumerate(daily_operations, start=1):

    print("-" * 60)
    print(f"Batch {day} starting")
    print("-" * 60)

    operations_simulator.process(transactions=transactions, batch_id=day)
    warehouse.direct_extract(data_generator.get_connection(), batch_id=day)
    warehouse.transform_load(batch_id=day)

print("\ndemo1.py completed successfully.")
