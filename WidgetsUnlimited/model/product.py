from .metadata import Table, Column


class ProductTable(Table):

    NAME = "product"

    def __init__(self):
        super().__init__(
            ProductTable.NAME,
            Column("product_id", "INTEGER", primary_key=True),
            Column("product_name", "VARCHAR"),
            Column("product_description", "VARCHAR"),
            Column("product_category", "VARCHAR", update=True),
            Column("product_brand", "VARCHAR"),
            Column("product_preferred_supplier_id", "INTEGER"),
            Column("product_unit_cost", "FLOAT"),
            Column("product_dimension_length", "FLOAT"),
            Column("product_dimension_width", "FLOAT"),
            Column("product_dimension_height", "FLOAT"),
            Column("product_introduced_date", "DATE"),
            Column("product_discontinued", "BOOLEAN"),
            Column("product_no_longer_offered", "BOOLEAN"),
            Column("product_inserted_at", "TIMESTAMP", inserted_at=True),
            Column("product_updated_at", "TIMESTAMP", updated_at=True),
        )
