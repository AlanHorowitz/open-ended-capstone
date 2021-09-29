import os
import sys

sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../WidgetsUnlimited")),
)

from warehouse.customer_dimension import CustomerDimensionProcessor
from warehouse.product_dimension import ProductDimensionProcessor
from warehouse.date_dimension import DateDimensionProcessor


from model.customer import CustomerTable
from model.customer_address import CustomerAddressTable

from model.product_dim import ProductDimTable
from model.date_dim import DateDimTable