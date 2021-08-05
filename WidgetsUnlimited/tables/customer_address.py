from util.sqltypes import Table, Column
from .customer import CustomerTable

class CustomerAddressTable(Table):

    NAME = "customer_address"

    def __init__(self):
        super().__init__(
            CustomerAddressTable.NAME,
            Column(
                "customer_id",
                "INTEGER",
                parent_table=CustomerTable.NAME,
                parent_key="customer_id",                
            ),           
            Column("customer_address_id", "INTEGER", isPrimaryKey=True),
            Column("customer_address", "VARCHAR", 255),
            Column("customer_address_type", "VARCHAR"),            
            Column("customer_address_inserted_at", "TIMESTAMP", isInsertedAt=True),
            Column("customer_address_updated_at", "TIMESTAMP", isUpdatedAt=True),
        )