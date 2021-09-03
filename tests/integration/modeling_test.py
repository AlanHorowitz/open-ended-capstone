# goal -- get one day of full model built with a small amount 
# of data so I can reasonably transform
import os
from datetime import datetime

from mysql.connector import connect

from context import CustomerDimensionProcessor

from context import DataGenerator, TableUpdate
from context import extract_write_stage


from context import ProductTable
from context import StoreTable
from context import StoreLocationTable
from context import StoreSalesTable
from context import OrderTable
from context import OrderLineItemTable
from context import CustomerTable
from context import CustomerAddressTable


from warehouse2 import create_and_copy_warehouse_tables, write_parquet_warehouse_tables

ms_connection = connect(
    host=os.getenv('WAREHOUSE_HOST'),
    port=os.getenv('WAREHOUSE_PORT'),
    user=os.getenv('WAREHOUSE_USER'),
    password=os.getenv('WAREHOUSE_PASSWORD'),
    database=os.getenv('WAREHOUSE_DB'),
    charset="utf8"
    )
data_generator : DataGenerator = DataGenerator()

product_table = ProductTable()
store_table = StoreTable()
store_sales_table = StoreSalesTable()
store_location_table = StoreLocationTable()
order_table = OrderTable()
order_line_item_table = OrderLineItemTable()
customer_table = CustomerTable()
customer_address_table = CustomerAddressTable()

customer_dimension = CustomerDimensionProcessor(ms_connection)

data_generator.add_tables([product_table, store_table, store_sales_table,
store_sales_table, store_location_table, order_table, order_line_item_table,
customer_table, customer_address_table])

data_generator.generate(
    TableUpdate(product_table, n_inserts=10))

# 5 stores with corresponding address
data_generator.generate(
    TableUpdate(store_table, n_inserts=5),1)
data_generator.generate(
    TableUpdate(store_location_table, n_inserts=1, link_parent=True), 1)

# 100 sales
data_generator.generate(
    TableUpdate(store_sales_table, n_inserts=100))

# 20 customers with corresponding address
data_generator.generate(
    TableUpdate(customer_table, n_inserts=20),1)
data_generator.generate(
    TableUpdate(customer_address_table, n_inserts=1,link_parent=True),1)

# 50 orders with 2 line items
data_generator.generate(
    TableUpdate(order_table, n_inserts=50),1)
data_generator.generate(
    TableUpdate(order_line_item_table, n_inserts=2, link_parent=True),1)

create_and_copy_warehouse_tables(data_generator.connection, [product_table, store_table, store_sales_table,
store_sales_table, store_location_table, order_table, order_line_item_table,
customer_table, customer_address_table])

extract_write_stage(1, [product_table, store_table, store_sales_table,
store_sales_table, store_location_table, order_table, order_line_item_table,
customer_table, customer_address_table])

customer_dimension.process_update(1)

# try modifying two customers
data_generator.generate(
    TableUpdate(customer_table, n_inserts=0, n_updates=2), batch_id=2)

extract_write_stage(2, [customer_table, customer_address_table])

customer_dimension.process_update(2)


