import os
import sys

sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../WidgetsUnlimited")),
)

from tables.sqltypes import Table, Column, DEFAULT_INSERT_VALUES

from tables.order import OrderTable
from tables.order_line_item import OrderLineItemTable
from tables.product import ProductTable
from tables.customer import CustomerTable
from operations.generator import DataGenerator
from operations.table_transaction import TableTransaction
