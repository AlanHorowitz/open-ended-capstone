import pytest
import os
from mysql.connector import connect

from .context import LocationDimensionProcessor
from .context import LocationDimTable


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
    region_name,
    location_name,
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
