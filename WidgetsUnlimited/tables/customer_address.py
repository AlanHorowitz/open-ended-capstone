from util.sqltypes import Table, Column

class CustomerAddressTable(Table):

    NAME = "customer_address"

    def __init__(self):
        super().__init__(
            CustomerAddressTable.NAME,
            Column("customer_address_id", "INTEGER", isPrimaryKey=True),
            Column("customer_address", "VARCHAR", 255),
            Column("customer_address_city", "VARCHAR"),
            Column("customer_address_password", "VARCHAR"),
            Column("customer_address_email", "VARCHAR"),            
            Column("customer_address_referral_type", "VARCHAR"),
            Column("customer_address_sex", "VARCHAR"),            
            Column("customer_address_date_of_birth", "DATE"),
            Column("customer_address_loyalty_number", "INTEGER"),
            Column("customer_address_is_preferred", "BOOLEAN"),
            Column("customer_address_is_active", "BOOLEAN"),
            Column("customer_address_inserted_at", "TIMESTAMP", isInsertedAt=True),
            Column("customer_address_updated_at", "TIMESTAMP", isUpdatedAt=True),
        )
