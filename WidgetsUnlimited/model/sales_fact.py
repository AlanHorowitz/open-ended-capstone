from .metadata import Table, Column


class SalesFactTable(Table):

    NAME = "sales_fact"

    def __init__(self):
        super().__init__(

            SalesFactTable.NAME,
            # dimension control
            Column("surrogate_key", "INTEGER", primary_key=True),  # surrogate key
            Column("effective_date", "DATE"),
            Column("expiration_date", "DATE"),
            Column("is_current_row", "BOOLEAN"),  # type 2 scd
            # customer columns
            Column("customer_key", "INTEGER"),  # natural key -- Add index
            create_only=True,
            batch_id=False,
        )