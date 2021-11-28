from WidgetsUnlimited.model.location_dim import LocationDimTable
from .warehouse_util import read_stage
import pandas as pd

from .dimension_processor import DimensionProcessor


# maintain a storage dimension table (what is in it?)
class LocationDimensionProcessor(DimensionProcessor):
    def __init__(self, connection=None):
        dim_table = LocationDimTable()
        dim_header_columns = dim_table.get_header_columns()
        super().__init__(connection, dim_table)
        if connection:
            self._create(StoreLocationStageTable())

        # set static columns from base and initialize aggregates.
        location_dim_base = pd.DataFrame(LocationDimTable.LOCATION_DATA)
        location_dim = pd.DataFrame([], columns=dim_table.get_column_names())
        location_dim[dim_header_columns] = location_dim_base[range(len(dim_header_columns))]

        location_dim['number_of_customers'] = 0
        location_dim['number_of_stores'] = 0
        location_dim['square_footage_of_stores'] = 0.0

        self._location_dim = location_dim.astype(dim_table.get_column_pandas_types()) \
            .set_index('surrogate_key', drop=False)

        if connection:
            self._write_table(None, self._location_dim, "INSERT")

    def process_update(self, batch_id: int) -> None:
        # apply location code for incremental store_location records
        location_dim = self._location_dim
        customer_locations = pd.read_sql_query(f'SELECT location_id from customer_dim', self._connection)
        store_data = pd.read_sql_query(f'SELECT location_id, square_footage from store_dim', self._connection)
        store_group = store_data.groupby('location_id')

        location_dim['number_of_customers'] = customer_locations['location_id'].value_counts()
        location_dim['number_of_customers'] = location_dim['number_of_customers'].fillna(0).astype(int)

        location_dim['number_of_stores'] = store_group['location_id'].count()
        location_dim['square_footage_of_stores'] = store_group['location_id'].mean()

        self._write_table(None, location_dim, "REPLACE")
