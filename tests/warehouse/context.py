import os
import sys

sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../WidgetsUnlimited")),
)

from warehouse.customer_dimension import CustomerDimensionProcessor
from warehouse.product_dimension import ProductDimensionProcessor


from model.customer import CustomerTable
from model.customer_address import CustomerAddressTable

from model.product_dim import ProductDimTable