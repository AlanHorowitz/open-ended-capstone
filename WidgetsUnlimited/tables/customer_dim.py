from util.sqltypes import Table, Column

class CustomerDimTable(Table):

    NAME = "customer_dim"

    def __init__(self):
        super().__init__(
            CustomerDimTable.NAME,

        # dimension control     
            Column("id", "INTEGER", isPrimaryKey=True),  # surrogate key
            Column("inserted_at", "VARCHAR"),
            Column("updated_at", "VARCHAR"),
            Column("is_current", "VARCHAR"),  # type 2 scd

        # customer columns
            Column("key", "INTEGER", isPrimaryKey=True),      # natural key 
            Column("name", "VARCHAR"),
            Column("user_id", "VARCHAR"),
            Column("password", "VARCHAR"),
            Column("email", "VARCHAR"),            
            Column("referral_type", "VARCHAR"),              # I can decode this int->string
            Column("sex", "VARCHAR"),               # ""            
            Column("date_of_birth", "DATE"),
            Column("loyalty_number", "INTEGER"),
            Column("credit_card_number", "VARCHAR"),
            Column("is_preferred", "BOOLEAN"),
            Column("is_active", "BOOLEAN"),
            Column("activation_date", "DATE", isInsertedAt=True),
            Column("deactivation_date", "DATE", isInsertedAt=True),            
            Column("billing_street_number", "INTEGER", isPrimaryKey=True),
            Column("billing_state", "VARCHAR", 255),
            Column("billing_zip", "VARCHAR"), 
            Column("billing_last_update", "TIMESTAMP", isUpdatedAt=True),
            Column("billing_number_of_updates", "INTEGER")  ,
            Column("shipping street number", "INTEGER", isPrimaryKey=True),
            Column("shipping state", "VARCHAR", 255),
            Column("shipping_zip", "VARCHAR"),   
            Column("shipping_last_update", "TIMESTAMP", isUpdatedAt=True),
            Column("shipping_number_of_updates", "INTEGER"))
              
             
        )
