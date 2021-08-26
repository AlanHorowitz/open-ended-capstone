from tables.product import ProductTable
from tables.customer import CustomerTable
from tables.customer_address import CustomerAddressTable
from tables.order import OrderTable
from tables.order_line_item import OrderLineItemTable

from .generator import DataGenerator 
from .ecommerce import eCommerceSystem
from .inventory import InventorySystem
from .table_update import TableUpdate
from .table_update_processor import TableUpdateProcessor


class SourceSystemLoader:
    """ Driver to load the source systems of Widgets Unlimited with sample data over a multi-day peridd.
        This data will be exposed in various formats as incremental updates to the data warehouse.

        phase #1 is confined to customers and orders in the e-commerce system
    """    
    def load(self) -> None:

        product_table = ProductTable()
        customer_table = CustomerTable()
        customer_address_table = CustomerAddressTable()
        order_table = OrderTable()
        order_line_item_table = OrderLineItemTable()

        data_generator = DataGenerator()

        e_commerce_system = eCommerceSystem(data_generator)
        inventory_system = InventorySystem(data_generator)

        # allocate the tables to tehir operational system.
        e_commerce_system.add_tables([customer_table,
        customer_address_table, order_table, order_line_item_table])
        inventory_system.add_tables([product_table])

        daily_operations = [
            [   
                # day 1  
                TableUpdate(product_table, n_inserts=5000, n_updates=0), 
                TableUpdate(customer_table, n_inserts=40, n_updates=0),
                TableUpdate(customer_address_table, n_inserts=40, n_updates=0),

            ],
            [
                # day 2
                TableUpdate(product_table, n_inserts=5, n_updates=50),
                TableUpdate(customer_table, n_inserts=40, n_updates=0),
                TableUpdate(customer_address_table, n_inserts=40, n_updates=0),
                TableUpdate(order_table, n_inserts=40, n_updates=0),
                TableUpdate(order_line_item_table, n_inserts=40, n_updates=0),
                
            ],
            [
                # day 3
                TableUpdate(product_table, n_inserts=5, n_updates=50),
                TableUpdate(customer_table, n_inserts=40, n_updates=0),
                TableUpdate(customer_address_table, n_inserts=40, n_updates=0),
                TableUpdate(order_table, n_inserts=40, n_updates=0),
                TableUpdate(order_line_item_table, n_inserts=40, n_updates=0),           ],
        ]

        table_update_processor = TableUpdateProcessor()

        for day in daily_operations:
            for table_update in day:
                table_update_processor.process(table_update)

        print("loader.load() completed sucessfully.")
            # phase #1 - write parquet
            # warehouse.direct_extract()
            # warehouse.transform_load()
            # phase #3 -- operational systems expose incremental changes    
            # warehouse.extract()
            # warehouse.transform_load()
            # phase #5 -- ping warehouse as independent system (container)
            # phase #6 -- no ping (warehouse ingests on its own schedule)
