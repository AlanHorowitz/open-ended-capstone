from util.sqltypes import Table, Column
from tables.customer import CustomerTable
from tables.customer_address import CustomerAddressTable

class OrderTable(Table):

    NAME = "order"

    def __init__(self):

        super().__init__(
            OrderTable.NAME,            
            Column("order_id", "INTEGER", isPrimaryKey=True),            
            Column(
                "customer_id",
                "INTEGER",
                xref_table=CustomerTable.NAME,
                xref_column="customer_id",
            ),
            Column(
                "order_shipping_address_id",
                "INTEGER",
                xref_table=CustomerAddressTable.NAME,
                xref_column="customer_address_id",
            ),
            Column("order_special_instructions", "VARCHAR"),
            Column("order_shipping_cost", "FLOAT"), 
            Column("order_execution_time", "TIMESTAMP"),
            Column("order_cancelled", "BOOLEAN"),
            Column("order_line_item_inserted_at", "TIMESTAMP", isInsertedAt=True),
            Column("order_line_item_updated_at", "TIMESTAMP", isUpdatedAt=True),
        )
