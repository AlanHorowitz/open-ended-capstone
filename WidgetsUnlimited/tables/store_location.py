from .sqltypes import Column, Table
from .store import StoreTable


class StoreLocationTable(Table):

    NAME = "store_location"

    def __init__(self):
        super().__init__(
            StoreLocationTable.NAME,
            Column("store_location_id", "INTEGER", primary_key=True),
            Column(
                "store_id",
                "INTEGER",
                parent_table=StoreTable.NAME,
                parent_key="store_id",
            ),
            Column("store_location_street_address", "VARCHAR", update=True),
            Column("store_location_city", "VARCHAR"),
            Column("store_location_state", "VARCHAR"),
            Column("store_location_zip_code", "VARCHAR"),
            Column("store_location_sq_footage", "FLOAT"),
            Column("store_inserted_at", "TIMESTAMP", inserted_at=True),
            Column("store_updated_at", "TIMESTAMP", updated_at=True),
        )
