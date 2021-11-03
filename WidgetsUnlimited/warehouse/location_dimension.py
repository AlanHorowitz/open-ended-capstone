from WidgetsUnlimited.model.location_dim import LocationDimTable
from .warehouse_util import read_stage
import pandas as pd

from .dimension_processor import DimensionProcessor
from WidgetsUnlimited.model.store_location_stage import StoreLocationStageTable
from WidgetsUnlimited.model.store_location import StoreLocationTable


class LocationDimensionProcessor(DimensionProcessor):
    def __init__(self, connection=None):
        dim_table = LocationDimTable()
        dim_header_columns = dim_table.get_header_columns()
        super().__init__(connection, dim_table)
        self._create(StoreLocationStageTable())

        # table headers plus zip into (tuple of ordered ranges e.g. ((0, 9999), (11000, 19999))

        # getting the static part of the location dimension -- what reading the tuples here accomplishes. I don't see
        zip_locations_df = pd.DataFrame(LocationDimTable.LOCATION_DATA,
                                        columns=dim_header_columns + ['zip_ranges'])
        location_dim = pd.DataFrame([], columns=dim_table.get_column_names())
        location_dim[dim_header_columns] = zip_locations_df[dim_header_columns]

        location_dim['number_of_customers'] = 0
        location_dim['number_of_stores'] = 0
        location_dim['square_footage_of_stores'] = 0.0

        self._location_dim = location_dim

        location_dim = location_dim.astype(dim_table.get_column_pandas_types())
        self._write_table(None, location_dim, "INSERT")

    def process_update(self, batch_id: int) -> None:
        # read incremental storage_location
        # read storage location stage table, pandas
        # apply updates
        # overwrite using pandas
        store_location, *_ = read_stage(
            batch_id, [StoreLocationTable(), ]
        )

        # get latest location code for new zips and save to disk
        # we aren't maintaining a store_dim at this point, so we will collect just what we need
        # in store_location_stage.
        store_location_stage = self._update_store_location_stage(store_location)
        self._write_table(StoreLocationStageTable(), store_location_stage, "REPLACE")

        # for each location category update the aggregates (how to calculate?)
        customer_zips = pd.read_sql_query(f'SELECT billing_zip from customer_dim', self._connection).billing_zip
        location_dim = self._update_location_dim(self._location_dim, store_location_stage, customer_zips)

        self._write_table(None, location_dim, "REPLACE")

    @staticmethod
    def _update_store_location_stage(store_location):
        store_location_stage = pd.DataFrame([])
        store_location_stage['store_id'] = store_location['store_id']
        store_location_stage['store_location_sq_footage'] = store_location['store_location_sq_footage']
        store_location_stage['location_id'] = (store_location['store_location_zip_code'].apply(int)). \
            apply(LocationDimTable.get_location_from_zip)
        return store_location_stage

    # aggregates to be figured out.
    @staticmethod
    def _update_location_dim(location_dim, store_location_stage, customer_zips):

        c = customer_zips[customer_zips != 'N/A'].apply(int).apply(LocationDimTable.get_location_from_zip)
        c.name = 'number_of_customers'
        df = pd.DataFrame(c.value_counts())

        location_dim['number_of_customers'] = df.number_of_customers
        location_dim['number_of_customers'].fillna(0, inplace=True)
        location_dim['number_of_stores'] = 0
        location_dim['square_footage_of_stores'] = 0.0

        return location_dim
