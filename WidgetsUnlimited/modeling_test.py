# goal -- get one day of full model built with a small amount 
# of data so I can reasonably transform
import os
from datetime import datetime

from sourcesystems.generator import DataGenerator, TableUpdate

from tables.product import ProductTable
from tables.store import StoreTable
from tables.store_location import StoreLocationTable
from tables.store_sales import StoreSalesTable
from tables.order import OrderTable
from tables.order_line_item import OrderLineItemTable
from tables.customer import CustomerTable
from tables.customer_address import CustomerAddressTable

from warehouse import create_and_copy_warehouse_tables

# test docker image
os.environ['DATA_GENERATOR_DB'] = 'postgres'
os.environ['DATA_GENERATOR_HOST'] = '172.17.0.2'
os.environ['DATA_GENERATOR_PORT'] = '5432'
os.environ['DATA_GENERATOR_USER'] = 'postgres'
os.environ['DATA_GENERATOR_PASSWORD'] = 'postgres'
os.environ['DATA_GENERATOR_SCHEMA'] = 'test'

data_generator : DataGenerator = DataGenerator()

product_table = ProductTable()
store_table = StoreTable()
store_sales_table = StoreSalesTable()
store_location_table = StoreLocationTable()
order_table = OrderTable()
order_line_item_table = OrderLineItemTable()
customer_table = CustomerTable()
customer_address_table = CustomerAddressTable()

print(store_location_table.get_create_sql_postgres())

data_generator.add_tables([product_table, store_table, store_sales_table,
store_sales_table, store_location_table, order_table, order_line_item_table,
customer_table, customer_address_table])

data_generator.generate(
    TableUpdate(product_table, n_inserts=10))

# 5 stores with corresponding address
data_generator.generate(
    TableUpdate(store_table, n_inserts=5,batch_id=1))
data_generator.generate(
    TableUpdate(store_location_table, n_inserts=1, batch_id=1, link_parent=True))

# 100 sales
data_generator.generate(
    TableUpdate(store_sales_table, n_inserts=100))

# 20 customers with corresponding address
data_generator.generate(
    TableUpdate(customer_table, n_inserts=20,batch_id=1))
data_generator.generate(
    TableUpdate(customer_address_table, n_inserts=1, batch_id=1, link_parent=True))

# 50 orders with 2 line items
data_generator.generate(
    TableUpdate(order_table, n_inserts=50,batch_id=1))
data_generator.generate(
    TableUpdate(order_line_item_table, n_inserts=2, batch_id=1, link_parent=True))

create_and_copy_warehouse_tables(data_generator.connection, [product_table, store_table, store_sales_table,
store_sales_table, store_location_table, order_table, order_line_item_table,
customer_table, customer_address_table])