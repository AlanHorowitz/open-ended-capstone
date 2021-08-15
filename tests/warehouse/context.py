import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../WidgetsUnlimited")))

from warehouse.customer_dim import CustomerDimension
    
from tables.customer import CustomerTable
from tables.customer_address import CustomerAddressTable


