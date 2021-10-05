from .metadata import Table, Column


class LocationDimTable(Table):

    NAME = "location_dim"

    def __init__(self):
        super().__init__(
            LocationDimTable.NAME,
            Column("surrogate_key", "INTEGER", primary_key=True),
            Column("region_name", "VARCHAR"),
            Column("location_name", "VARCHAR"),
            Column("number_of_stores", "INTEGER"),
            Column("square_footage_of_stores", "FLOAT"),
            create_only=True,
            batch_id=False,
        )

    def get_header_columns(self):
        return [col.get_name() for col in self.get_columns()[0:2]]