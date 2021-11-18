from .metadata import Column, Table


class StoreLocationStageTable(Table):

    NAME = "store_location_stage"

    # Keep track of location_id and sq footage by store_id, perhaps work file would be better
    # not to overload the word stage.
    def __init__(self):
        super().__init__(
            StoreLocationStageTable.NAME,
            Column("store_id", "INTEGER", primary_key=True),
            Column("location_id", "INTEGER"),
            Column("store_location_sq_footage", "FLOAT"),
            create_only=True,
            batch_id=False,
        )
