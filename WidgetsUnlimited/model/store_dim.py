from .metadata import Table, Column


class StoreDimTable(Table):
    """ This is abbreviated Store data only used to keep track of data used by Location Dimension """

    NAME = "store_dim"

    def __init__(self):
        super().__init__(
            StoreDimTable.NAME,
            # dimension control header columns
            Column("surrogate_key", "INTEGER", primary_key=True),  # surrogate key
            Column("effective_date", "DATE"),
            Column("expiration_date", "DATE"),
            Column("is_current_row", "BOOLEAN"),  # type 2 scd
            # store columns
            Column("store_key", "INTEGER"),  # natural key -- Add index
            Column("name", "VARCHAR"),
            Column("location_id", "INTEGER"),
            Column("square_footage", "FLOAT"),
            create_only=True,
            batch_id=False,
        )

    def get_header_columns(self):
        return [col.get_name() for col in self.get_columns()[0:4]]
