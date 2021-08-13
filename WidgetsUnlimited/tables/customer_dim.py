from util.sqltypes import Table, Column

class CustomerDimTable(Table):

    NAME = "customer_dim"

    def __init__(self):
        super().__init__(
            CustomerDimTable.NAME,            

        # dimension control     
            Column("id", "INTEGER", isPrimaryKey=True),  # surrogate key
            Column("inserted_at", "TIMESTAMP"),
            Column("updated_at", "TIMESTAMP"),
            Column("is_current", "BOOLEAN"),  # type 2 scd

        # customer columns
            Column("customer_key", "INTEGER"),      # natural key -- Add index
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
            Column("activation_date", "DATE"),
            Column("deactivation_date", "DATE"),  
            Column("billing_name", "VARCHAR", 255),         
            Column("billing_street_number", "INTEGER",),
            Column("billing_city", "VARCHAR", 255),
            Column("billing_state", "VARCHAR", 255),
            Column("billing_zip", "VARCHAR"), 
            Column("billing_last_update", "TIMESTAMP"),
            Column("billing_number_of_updates", "INTEGER")  ,
            Column("shipping_name", "VARCHAR", 255),
            Column("shipping_street_number", "VARCHAR"),
            Column("shipping_city", "VARCHAR", 255),
            Column("shipping_state", "VARCHAR", 255),
            Column("shipping_zip", "VARCHAR"),   
            Column("shipping_last_update", "TIMESTAMP"),
            Column("shipping_number_of_updates", "INTEGER"),
            generation=False)
              
             
    
