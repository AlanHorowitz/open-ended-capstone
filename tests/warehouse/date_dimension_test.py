import os
from datetime import date, datetime
import pandas as pd
import numpy as np
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


def test_build_dimension():

    p = DateDimensionProcessor(None)

    date_dim = p._build_dimension(DATE_DIMENSION_START, DATE_DIMENSION_END)

    assert date_dim['date'].min() == DATE_DIMENSION_START
    assert date_dim['date'].max() == DATE_DIMENSION_END

    assert date_dim.loc[np.datetime64('2021-10-01'), 'date'] == datetime(2021, 10, 1)
    assert date_dim.loc[np.datetime64('2021-10-01'), 'day_name'] == 'Friday'
    assert date_dim.loc[np.datetime64('2021-10-01'), 'month_name'] == 'October'
    assert date_dim.loc[np.datetime64('2021-10-01'), 'year_name'] == 2021
    assert date_dim.loc[np.datetime64('2021-10-01'), 'day_number_in_month'] == 1
    assert date_dim.loc[np.datetime64('2021-10-01'), 'day_number_in_year'] == 274
    assert date_dim.loc[np.datetime64('2021-10-01'), 'date_text_description'] == 'October 01, 2021'
    assert date_dim.loc[np.datetime64('2021-10-01'), 'weekday_indicator'] == 'WEEKDAY'
    assert date_dim.loc[np.datetime64('2021-10-01'), 'holiday_indicator'] == 'NOT HOLIDAY'

    weekdays = date_dim.loc[np.datetime64('2021-09-27'):np.datetime64('2021-10-01'), 'weekday_indicator']
    assert weekdays.size == 5
    assert weekdays.nunique() == 1
    assert weekdays[0] == "WEEKDAY"

    weekends = date_dim.loc[np.datetime64('2021-10-02'):np.datetime64('2021-10-03'), 'weekday_indicator']
    assert weekends.size == 2
    assert weekends.nunique() == 1
    assert weekends[0] == "WEEKEND"

    holidays = date_dim.loc[[np.datetime64('2021-11-25'), np.datetime64('2021-12-24')], 'holiday_indicator']
    assert holidays.size == 2
    assert holidays.nunique() == 1
    assert holidays[0] == "HOLIDAY"


def test_build_dimension_persist(ms_connection):
    c = DateDimensionProcessor(ms_connection)
