import os
from datetime import date, datetime
import pandas as pd
import pytest

# from WidgetsUnlimited.warehouse.warehouse_util import get_new_keys
from mysql.connector import connect

from .context import StoreDimensionProcessor, StoreTable, StoreLocationTable, StoreDimTable

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


def test_dimension_transform():
    store_dim = pd.DataFrame([], columns=StoreDimTable().get_column_names(), index=range(1, 8))
    store = pd.DataFrame(store_data).set_index("store_id", drop=False)
    store_location = pd.DataFrame(store_location_data).set_index("store_id", drop=False)

    store_dim = StoreDimensionProcessor.dimension_transform(store_dim, store, store_location)

    assert len(store_dim) == 7
    assert store_dim.loc[5:7, 'location_id'].to_list() == [9, 11, 12]
    assert store_dim.loc[5:7, 'square_footage'].to_list() == [2500, 3000, 3500]


def test_build_new_dimension():
    dim_processor = StoreDimensionProcessor(None)

    store = pd.DataFrame(store_data).set_index("store_id", drop=False)
    store_location = pd.DataFrame(store_location_data).set_index("store_id", drop=False)
    new_keys = pd.Index(range(1, 8))
    inserts = dim_processor._build_new_dimension(new_keys, store, store_location)

    assert len(inserts) == 7
    assert inserts.loc[2, 'effective_date'] == date(2020, 10, 10)
    assert inserts.loc[2, 'expiration_date'] == date(2099, 12, 31)
    assert inserts.loc[2, 'is_current_row'] == True
    assert inserts.loc[5:7, 'surrogate_key'].to_list() == [5, 6, 7]
    assert inserts.loc[5:7, 'store_key'].to_list() == [5, 6, 7]
    assert inserts.loc[5:7, 'name'].to_list() == ["S5", "S6", "S7"]
    assert inserts.loc[5:7, 'location_id'].to_list() == [9, 11, 12]
    assert inserts.loc[5:7, 'square_footage'].to_list() == [2500, 3000, 3500]


def test_build_update_dimension():
    dim_processor = StoreDimensionProcessor(None)

    update_keys = pd.Index([1, 2])
    updates = dim_processor._build_update_dimension(
        update_keys, prior_store_dim, store, store_location
    )