from model.location_dim import LocationDimTable
from .warehouse_util import read_stage


from .dimension_processor import DimensionProcessor
from model.store_location_stage import StoreLocationStageTable
from model.store_location import StoreLocationTable



class LocationDimensionProcessor(DimensionProcessor):
    def __init__(self, connection=None):
        super().__init__(connection, LocationDimTable())
        self._create(StoreLocationStageTable())

    def process_update(self, batch_id: int) -> None:
        # read incremental storage_location
        # read storage location stage table, pandas
        # apply updates
        # overwrite using pandas
        store_location = read_stage(
            batch_id, [StoreLocationTable(), ]
        )
        store_location_stage = self._apply_stage_updates(store_location)

    def _apply_stage_updates(self, store_location):
        pass
