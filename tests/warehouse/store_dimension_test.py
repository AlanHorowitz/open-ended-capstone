import os
from datetime import date, datetime
import pandas as pd
import pytest

# from WidgetsUnlimited.warehouse.warehouse_util import get_new_keys
from mysql.connector import connect

from .context import StoreDimensionProcessor, StoreTable, StoreLocationTable, StoreDimTable


def test_init():

    store_data = {
        "store_id": [1, 2, 3, 4, 5, 6, 7],
        "store_name": ["S" + str(a) for a in range(1, 8)],
        "store_manager_name": ["M" + str(a) for a in range(1, 8)],
        "store_number_of_employees": [10] * 7,
        "store_opened_date": [None] * 7,
        "store_closed_date": [None] * 7,
        "store_inserted_at": [None] * 7,
        "store_updated_at": [None] * 7,
        "batch_id": [1] * 7,
    }

    store_location_data = {
        "store_location_id": [1, 2, 3, 4, 5, 6, 7],
        "store_id": [1, 2, 3, 4, 5, 6, 7],
        "store_location_street_address": ["XX"] * 7,
        "store_location_city": ["YY"] * 7,
        "store_location_state": ["NY"] * 7,
        "store_location_zip_code": [11111 * a for a in range(1, 8)],
        "store_location_sq_footage": [500 * a for a in range(1, 8)],
        "store_inserted_at": [None] * 7,
        "store_updated_at": [None] * 7,
        "batch_id": [1] * 7,
    }

    sp = StoreDimensionProcessor(None)

    store_dim = pd.DataFrame(
        [], columns=StoreDimTable().get_column_names(), index=range(1, 8)
    )

    store = pd.DataFrame(store_data).set_index("store_id", drop=False)
    store_location = pd.DataFrame(store_location_data).set_index("store_id", drop=False)

    sp.dimension_transform(store_dim, store, store_location)

    store_dim = sp.dimension_transform(store_dim, store, store_location)

    assert len(store_dim) == 7
    assert store_dim.loc[5:7, 'location_id'].to_list() == [9, 11, 12]
    assert store_dim.loc[5:7, 'square_footage'].to_list() == [2500, 3000, 3500]

