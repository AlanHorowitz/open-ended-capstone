from psycopg2.extensions import TRANSACTION_STATUS_IDLE
from .metadata import Table, Column
from .customer import CustomerTable
from .default_functions import name_address_generator


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
            Column("customer_address_id", "INTEGER", primary_key=True),
            Column(
                "customer_address",
                "VARCHAR",
                255,
                # default="First Middle Last\n123 Snickersnack Lane\nBrooklyn, NY 11229",
                default=name_address_generator,
                update=True,
            ),
            # Column("customer_temp_updateable", "VARCHAR", update=True),
            Column("customer_address_type", "VARCHAR", default="B"),
            Column("customer_address_inserted_at", "TIMESTAMP", inserted_at=True),
            Column("customer_address_updated_at", "TIMESTAMP", updated_at=True),
        )
