"""A simulated data warehouse for the retail domain.

The data from 3 different source systems representing e-commerce, in-store sales,
and inventory will be consolidated into a data warehouse.  
"""
import os
from logging.config import fileConfig

conf = os.environ.get("WIDGETS_LOG_CONF")
if conf:
    fileConfig(conf)
else:
    print("Error no log configuration found")
    exit(1)
