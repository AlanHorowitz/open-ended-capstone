from .metadata import Column, Table


class StoreLocationStageTable(Table):

    NAME = "store_location_stage"

    def __init__(self):
        super().__init__(
            StoreLocationStageTable.NAME,
            Column("store_id", "INTEGER", primary_key=True),
            Column("location_id", "INTEGER"),
            Column("store_location_sq_footage", "FLOAT"),
            create_only=True,
            batch_id=False,
        )
