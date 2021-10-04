from .metadata import Table, Column


class ProductDimTable(Table):

    NAME = "product_dim"

    def __init__(self):
        super().__init__(
            ProductDimTable.NAME,
            # dimension control header columns
            Column("surrogate_key", "INTEGER", primary_key=True),  # surrogate key
            Column("effective_date", "DATE"),
            Column("expiration_date", "DATE"),
            Column("is_current_row", "BOOLEAN"),  # type 2 scd
            # product columns
            Column("product_key", "INTEGER"),  # natural key -- Add index
            Column("name", "VARCHAR"),
            Column("description", "VARCHAR"),
            Column("category", "VARCHAR", update=True),
            Column("brand", "VARCHAR"),
            # Column("product_preferred_supplier_id", "INTEGER"),
            Column("unit_cost", "FLOAT"),
            Column("dimension_length", "FLOAT"),
            Column("dimension_width", "FLOAT"),
            Column("dimension_height", "FLOAT"),
            Column("introduced_date", "DATE"),
            Column("discontinued", "BOOLEAN"),
            Column("no_longer_offered", "BOOLEAN"),
            Column("number_of_suppliers", "INTEGER"),
            Column("percent_returns", "FLOAT"),
            create_only=True,
            batch_id=False,
        )

    def get_header_columns(self):
        return [col.get_name() for col in self.get_columns()[0:4]]
