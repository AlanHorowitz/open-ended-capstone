import os
import sys

sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../WidgetsUnlimited")),
)

from warehouse.customer_dimension import CustomerDimension

from tables.customer import CustomerTable
from tables.customer_address import CustomerAddressTable

from sourcesystems.generator import DataGenerator, TableUpdate

from tables.product import ProductTable
from tables.store import StoreTable
from tables.store_location import StoreLocationTable
from tables.store_sales import StoreSalesTable
from tables.order import OrderTable
from tables.order_line_item import OrderLineItemTable
