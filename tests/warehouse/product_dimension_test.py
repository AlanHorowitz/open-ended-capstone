import os
from datetime import date, datetime
import pandas as pd
import pytest

# from WidgetsUnlimited.warehouse.warehouse_util import get_new_keys
from mysql.connector import connect

from .context import ProductDimensionProcessor
from .context import ProductDimTable


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


# Build new customer_dim from customers and addresses.
# Customers 3 & 5 have billing and shipping; customer 4, billing only,
def test_build_new_dimension():

    c = ProductDimensionProcessor(None)

    _day = date(2020, 10, 10)
    _date = datetime.now()

    product_data = {

        "product_id": [1, 2, 3, 4],
        "product_name": ["p1", "p2", "p3", "p4"],
        "product_description": ["XXX"] * 4,
        "product_category": ["cat"] * 4,
        "product_brand": ["b1", "b2", "b3", "b4"],
        "product_unit_cost": [10, 20, 30, 40],
        "product_dimension_length": [1, 2, 3, None],
        "product_dimension_width": [10.5] * 4,
        "product_dimension_height": [2.7] * 4,
        "product_introduced_date": [_day] * 4,
        "product_discontinued": [True, False, True, True],
        "product_no_longer_offered": [True] * 4,
        "product_inserted_at": [_date] * 4,
        "product_updated_at": [_date] * 4,
        "batch_id": [1] * 4,
    }

    product_supplier_data = {

        "product_id":  [1, 1, 1, 2, 3, 3, 4, 4, 4, 4],
        "supplier_id": [1, 2, 3, 1, 1, 2, 1, 2, 3, 7],
        "product_supplier_inserted_at": [_date] * 10,
        "product_supplier_updated_at": [_date] * 10,
        "batch_id": [1] * 10,
    }

    new_keys = pd.Index([3, 4])

    product = pd.DataFrame(product_data).set_index("product_id", drop=False)
    product_supplier = pd.DataFrame(product_supplier_data).set_index(
        "product_id", drop=False
    )

    new_records = c._build_new_dimension(new_keys, product, product_supplier)

    assert new_records.shape[0] == 2
    assert new_records.at[3, "number_of_suppliers"] == 2
    assert new_records.at[4, "number_of_suppliers"] == 4
    assert new_records.at[3, "unit_cost"] == 30.0
    assert new_records.at[4, "brand"] == "b4"

    assert sorted(new_records.columns) == sorted(ProductDimTable().get_column_names())
    assert new_records['surrogate_key'].to_list() == [1, 2]



