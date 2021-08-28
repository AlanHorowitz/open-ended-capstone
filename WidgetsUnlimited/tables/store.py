from util.sqltypes import Column, Table


class StoreTable(Table):

    NAME = "store"

    def __init__(self):
        super().__init__(
            StoreTable.NAME,
            Column("store_id", "INTEGER", isPrimaryKey=True),
            Column("store_name", "VARCHAR"),
            Column("store_manager_name", "VARCHAR", isUpdateable=True),
            Column("store_number_of_employees", "INTEGER"),
            Column("store_opened_date", "DATE"),
            Column("store_closed_date", "DATE"),
            Column("store_inserted_at", "TIMESTAMP", isInsertedAt=True),
            Column("store_updated_at", "TIMESTAMP", isUpdatedAt=True),
        )
