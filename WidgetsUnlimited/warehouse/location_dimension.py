import pandas as pd

from pandas.core.frame import DataFrame
from model.location_dim import LocationDimTable

from .dimension_processor import DimensionProcessor


class LocationDimensionProcessor(DimensionProcessor):
    def __init__(self, connection=None):

        super().__init__(connection, LocationDimTable())

    def build_dimension(self, start_date: date, end_date: date):
        """
        Initialize a static date dimension and write to mySQL. Primary key is native DATE format.
        :param start_date: starting date of dimension
        :param end_date: ending date (inclusive) of dimension
        """

        date_dim = self._build_dimension(start_date, end_date)
        self._write_dimension(date_dim, "INSERT")

    def _build_dimension(self, start_date: date, end_date: date) -> DataFrame:
        """
        Create a dataframe representing the date dimension.  Use pandas functions to isolate
        derived properties of each date.
        :param start_date: starting date of dimension
        :param end_date:
        :return: ending date (inclusive) of dimension
        """
        date_dim = pd.DataFrame([], columns=self._dimension_table.get_column_names())
        dr = pd.date_range(start=start_date, end=end_date)

        holidays = HolidayCalendar().holidays(start=dr.min(), end=dr.max())
        date_dim["date"] = dr

        date_dim["day_name"] = date_dim["date"].dt.day_name()
        date_dim["month_name"] = date_dim["date"].dt.month_name()
        date_dim["year_name"] = date_dim["date"].dt.year
        date_dim["day_number_in_month"] = date_dim["date"].dt.day
        date_dim["day_number_in_year"] = date_dim["date"].dt.day_of_year
        date_dim["date_text_description"] = date_dim["date"].dt.strftime("%B %d, %Y")
        date_dim["weekday_indicator"] = np.where(
            date_dim["date"].dt.dayofweek < 5, "WEEKDAY", "WEEKEND"
        )
        date_dim["holiday_indicator"] = np.where(
            date_dim["date"].isin(holidays), "HOLIDAY", "NOT HOLIDAY"
        )

        return date_dim.set_index("date", drop=False)
