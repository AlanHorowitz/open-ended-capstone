from util.sqltypes import Column, Table
from .store import StoreTable

class StoreLocationTable(Table):

    NAME = "store_location"

    def __init__(self):
        super().__init__(
            StoreLocationTable.NAME,
            Column("store_location_id", "INTEGER", isPrimaryKey=True),            
            Column(
                "store_id",
                "INTEGER",
                parent_table=StoreTable.NAME,
                parent_key="store_id",                
            ),           
            Column("store_location_street_address", "VARCHAR"),
            Column("store_location_city", "VARCHAR"),
            Column("store_location_state", "VARCHAR"),
            Column("store_location_zip_code", "VARCHAR"),
            Column("store_location_sq_footage", "FLOAT"),            
            Column("store_inserted_at", "TIMESTAMP", isInsertedAt=True),
            Column("store_updated_at", "TIMESTAMP", isUpdatedAt=True),
        )
