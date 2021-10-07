from model.location_dim import LocationDimTable
from .warehouse_util import read_stage
import pandas as pd

from .dimension_processor import DimensionProcessor
from model.store_location_stage import StoreLocationStageTable
from model.store_location import StoreLocationTable


class LocationDimensionProcessor(DimensionProcessor):
    def __init__(self, connection=None):
        dim_table = LocationDimTable()
        dim_header_columns = dim_table.get_header_columns()
        super().__init__(connection, dim_table)
        self._create(StoreLocationStageTable())
        # table headers plus zip into
        zip_locations_df = pd.DataFrame(dim_table.get_location_data(),
                                        list(dim_header_columns).append('zip_ranges')) \
            .drop('zip_ranges', axis='columns')
        location_dim = pd.DataFrame([], columns=dim_table.get_column_names)
        location_dim[dim_header_columns] = zip_locations_df[dim_header_columns]
        location_dim['number_of_customers'] = 0
        location_dim['number_of_stores'] = 0
        location_dim['square_footage_of_stores'] = 0.0

        location_dim = location_dim.astype(dim_table.get_column_pandas_types())
        self._write_dimension(location_dim)



    def process_update(self, batch_id: int) -> None:
        # read incremental storage_location
        # read storage location stage table, pandas
        # apply updates
        # overwrite using pandas
        store_location = read_stage(
            batch_id, [StoreLocationTable(), ]
        )
        self._apply_stage_updates(store_location)

    def _apply_stage_updates(self, store_location):
        n = LocationDimTable.get_location_from_zip(2)
        pass
