import pandas as pd
import pytest
import os
from mysql.connector import connect
from datetime import datetime

from .context import LocationDimensionProcessor
from .context import LocationDimTable

_date = datetime.now()


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


# see if location.dim and store_location_stage are properly initialized
def test_init_dimension(ms_connection):
    p = LocationDimensionProcessor(ms_connection)
    cur = ms_connection.cursor()

    cur.execute("SELECT COUNT(*) from store_location_stage;")
    row = cur.fetchone()

    assert row[0] == 0

    cur.execute("SELECT COUNT(*) from location_dim;")
    row = cur.fetchone()

    assert row[0] == 15

    select_sql = """
    SELECT 
    surrogate_key,
    location_name,
    region_name,
    number_of_customers,
    number_of_stores,
    square_footage_of_stores
    FROM location_dim
    WHERE surrogate_key = 11;
    """
    cur.execute(select_sql)
    rows = cur.fetchall()

    assert len(rows) == 1
    assert rows[0][0] == 11
    assert rows[0][1] == "LOC11"
    assert rows[0][2] == "REG5"
    assert rows[0][3] == 0
    assert rows[0][4] == 0
    assert rows[0][5] == 0.0


def test_get_location_from_zip():
    assert LocationDimTable.get_location_from_zip(12345) == 1
    assert LocationDimTable.get_location_from_zip(80000) == 13
    assert LocationDimTable.get_location_from_zip(87999) == 13
    with pytest.raises(Exception):
        LocationDimTable.get_location_from_zip(111111)


def test_update_store_location_stage():
    store_location_data = {
        "store_id": [1, 2, 3, 4],
        "store_location_street_address": ['10 Oak', '20 Oak', '30 Oak', '40 Oak'],
        "store_location_state": ['NY', 'NJ', 'PA', 'CT'],
        "store_location_zip_code": ["11111", "22222", "33333", "44444"],
        "store_location_sq_footage": [10000, 20000, 30000, 40000],
        "customer_address_inserted_at": [_date] * 4,
        "customer_address_updated_at": [_date] * 4,
        "batch_id": [1] * 4,
    }

    store_location = pd.DataFrame(store_location_data)
    store_location_stage = LocationDimensionProcessor._update_store_location_stage(store_location)

    assert store_location_stage.iloc[0]['store_id'] == 1
    assert store_location_stage.iloc[0]['store_location_sq_footage'] == 10000
    assert store_location_stage.iloc[0]['location_id'] == 1

    assert store_location_stage.iloc[3]['store_id'] == 4
    assert store_location_stage.iloc[3]['store_location_sq_footage'] == 40000
    assert store_location_stage.iloc[3]['location_id'] == 7


def test_update_location_dim():
    location_dim = LocationDimensionProcessor()._location_dim
    customer_zips = pd.Series(['11111', '22222', '24000', '27999', '28000'])
    LocationDimensionProcessor._update_location_dim(location_dim, None, customer_zips)

    assert location_dim.loc[1, 'number_of_customers'] == 1
    assert location_dim.loc[2, 'number_of_customers'] == 1
    assert location_dim.loc[3, 'number_of_customers'] == 2
    assert location_dim.loc[4, 'number_of_customers'] == 1
    assert location_dim.loc[5, 'number_of_customers'] == 0
