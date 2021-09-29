import os
from datetime import date, datetime
import pandas as pd
import pytest

# from WidgetsUnlimited.warehouse.warehouse_util import get_new_keys
from mysql.connector import connect

from .context import DateDimensionProcessor
from .context import DateDimTable


@pytest.fixture
def ms_connection():
    yield connect(
        host=os.getenv("WAREHOUSE_HOST"),
        port=os.getenv("WAREHOUSE_PORT"),
        user=os.getenv("WAREHOUSE_USER"),
        password=os.getenv("WAREHOUSE_PASSWORD"),
        database=os.getenv("WAREHOUSE_DB"),
        charset="utf8",
    )


DATE_DIMENSION_START = date(2020, 1, 1)
DATE_DIMENSION_END = date(2024, 12, 31)


# Build new customer_dim from customers and addresses.
# Customers 3 & 5 have billing and shipping; customer 4, billing only,
def test_build_dimension():

    p = DateDimensionProcessor(None)

    date_dim = p._build_dimension(DATE_DIMENSION_START, DATE_DIMENSION_END)


def test_build_dimension_persist(ms_connection):

    c = DateDimensionProcessor(ms_connection)
