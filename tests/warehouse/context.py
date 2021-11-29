import os
import sys

sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../WidgetsUnlimited")),
)

from warehouse.customer_dimension import CustomerDimensionProcessor
from warehouse.product_dimension import ProductDimensionProcessor
from warehouse.date_dimension import DateDimensionProcessor
from warehouse.location_dimension import LocationDimensionProcessor
from warehouse.store_dimension import StoreDimensionProcessor


from model.customer import CustomerTable
from model.customer_address import CustomerAddressTable
from model.store import StoreTable
from model.store_location import StoreLocationTable

from model.product_dim import ProductDimTable
from model.date_dim import DateDimTable
from model.location_dim import LocationDimTable
from model.store_dim import StoreDimTable