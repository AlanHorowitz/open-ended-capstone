from util.sqltypes import Table, Column

class CustomerTable(Table):

    NAME = "customer"

    def __init__(self):
        super().__init__(
            CustomerTable.NAME,
            Column("customer_id", "INTEGER", isPrimaryKey=True),
            Column("customer_name", "VARCHAR"),
            Column("customer_user_id", "VARCHAR"),
            Column("customer_password", "VARCHAR"),
            Column("customer_email", "VARCHAR"),            
            Column("customer_referral_type", "VARCHAR"),
            Column("customer_sex", "VARCHAR"),            
            Column("customer_date_of_birth", "DATE"),
            Column("customer_loyalty_number", "INTEGER"),
            Column("customer_credit_card_number", "VARCHAR"),
            Column("customer_is_preferred", "BOOLEAN"),
            Column("customer_is_active", "BOOLEAN"),
            Column("customer_inserted_at", "TIMESTAMP", isInsertedAt=True),
            Column("customer_updated_at", "TIMESTAMP", isUpdatedAt=True),
        )