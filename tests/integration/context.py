import os
import sys

sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../WidgetsUnlimited")),
)

from warehouse.customer_dimension import CustomerDimensionProcessor
from warehouse.warehouse_util import extract_write_stage
from warehouse.data_warehouse import DataWarehouse

from tables.customer import CustomerTable
from tables.customer_address import CustomerAddressTable

from sourcesystems.generator import DataGenerator
from sourcesystems.table_transaction import TableTransaction

from tables.product import ProductTable
from tables.store import StoreTable
from tables.store_location import StoreLocationTable
from tables.store_sales import StoreSalesTable
from tables.order import OrderTable
from tables.order_line_item import OrderLineItemTable
