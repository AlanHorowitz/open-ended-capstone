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


def test_build_update_dimension():

    c = ProductDimensionProcessor(None)

    _day = date(2020, 10, 10)
    _date = datetime.now()

    product_data = {

        "product_id": [11, 22, 33, 44],
        "product_name": ["p11", "p22", "p33", "p44"],
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

        "product_id":  [11, 11, 11, 22, 33, 33, 44, 44, 44, 44],
        "supplier_id": [1, 2, 3, 1, 1, 2, 1, 2, 3, 7],
        "product_supplier_inserted_at": [_date] * 10,
        "product_supplier_updated_at": [_date] * 10,
        "batch_id": [1] * 10,
    }

    product_dimension_data = {

        "surrogate_key": [1, 2],
        "effective_date": [date(2020, 10, 10), date(2020, 10, 10)],
        "expiration_date": [date(2099, 12, 31), date(2099, 12, 31)],
        "is_current_row": [True, True],
        "product_key": [11, 22],
        "name": ["p11", "p22"],
        "description": ["XXX"] * 2,
        "category": ["cat"] * 2,
        "brand": ["b1", "b2"],
        "unit_cost": [10, 20],
        "dimension_length": [1, 2],
        "dimension_width": [10.5] * 2,
        "dimension_height": [2.7] * 2,
        "introduced_date": [_day] * 2,
        "discontinued": [True, False],
        "no_longer_offered": [True] * 2,
        "number_of_suppliers": [7,7],
        "percent_returns": [.5] * 2,
    }

    prior_product_dim = pd.DataFrame(product_dimension_data).set_index("product_key", drop=False)
    update_keys = prior_product_dim.index

    product = pd.DataFrame(product_data).set_index("product_id", drop=False).loc[update_keys]
    product_supplier = pd.DataFrame(product_supplier_data).set_index(
        "product_id", drop=False
    ).loc[update_keys]

    update_records = c._build_update_dimension(update_keys, prior_product_dim, product, product_supplier)

    assert update_records.shape[0] == 2
    assert update_records.at[11, "number_of_suppliers"] == 3
    assert update_records.at[22, "number_of_suppliers"] == 1
    assert update_records.at[11, "surrogate_key"] == 1
    assert update_records.at[22, "surrogate_key"] == 2
    assert update_records.at[11, "unit_cost"] == 10.0
    assert update_records.at[22, "brand"] == "b2"

    assert sorted(update_records.columns) == sorted(ProductDimTable().get_column_names())
    assert update_records['surrogate_key'].to_list() == [1, 2]
