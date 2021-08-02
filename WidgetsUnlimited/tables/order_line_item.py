from util.sqltypes import Table, Column
from .product import ProductTable
from .order import OrderTable

class OrderLineItemTable(Table):

    NAME = "order_line_item"
    
    def __init__(self):

        super().__init__(
            OrderLineItemTable.NAME,            
            Column("order_line_item_id", "INTEGER", isPrimaryKey=True), 
            Column(
                "order_id",
                "INTEGER",
                parent_table=OrderTable.NAME,
                parent_key="order_id",                
            ),           
            # Column(
            #     "order_line_item_product_id",
            #     "INTEGER",
            #     xref_table=ProductTable.NAME,
            #     xref_column="product_id",
            # ),
            Column("order_line_item_quantity", "INTEGER"),
            # Column(
            #     "order_line_item_unit_price",
            #     "FLOAT",
            #     xref_table=ProductTable.NAME,
            #     xref_column="product_unit_cost",
            # ),
            Column("order_line_item_total_price", "FLOAT"),
            Column("order_line_comments", "VARCHAR"),            
            Column("order_line_item_inserted_at", "TIMESTAMP", isInsertedAt=True),
            Column("order_line_item_updated_at", "TIMESTAMP", isUpdatedAt=True),
        )
