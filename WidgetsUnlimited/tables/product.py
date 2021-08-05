from util.sqltypes import Table, Column

class ProductTable(Table):

    NAME = "product"    
    
    def __init__(self):
        super().__init__(
            ProductTable.NAME,            
            Column("product_id", "INTEGER", isPrimaryKey=True),
            Column("product_name", "VARCHAR"),
            Column("product_description", "VARCHAR"),
            Column("product_category", "VARCHAR"),
            Column("product_brand", "VARCHAR"),
            Column("product_preferred_supplier_id", "INTEGER"),
            Column("product_unit_cost", "FLOAT"),
            Column("product_dimension_length", "FLOAT"),
            Column("product_dimension_width", "FLOAT"),
            Column("product_dimension_height", "FLOAT"),
            Column("product_introduced_date", "DATE"),
            Column("product_discontinued", "BOOLEAN"),
            Column("product_no_longer_offered", "BOOLEAN"),
            Column("product_inserted_at", "TIMESTAMP", isInsertedAt=True),
            Column("product_updated_at", "TIMESTAMP", isUpdatedAt=True),
        )
