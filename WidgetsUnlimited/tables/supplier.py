from util.sqltypes import Table, Column


class SupplierTable(Table):

    NAME = "supplier"

    def __init__(self):
        super().__init__(
            SupplierTable.NAME,
            Column("supplier_id", "INTEGER", isPrimaryKey=True),
            Column("supplier_name", "VARCHAR"),
            Column("supplier_address", "VARCHAR"),
            Column("supplier_primary_contact_name", "VARCHAR", isUpdateable=True),
            Column("supplier_primary_contact_phone", "VARCHAR"),
            Column("supplier_secondary_contact_name", "VARCHAR"),
            Column("supplier_secondary_contact_phone", "VARCHAR"),
            Column("supplier_web_site", "VARCHAR"),
            Column("supplier_introduction_date", "DATE"),
            Column("supplier_is_preferred", "BOOLEAN"),
            Column("supplier_is_active", "BOOLEAN"),
            Column("supplier_inserted_at", "TIMESTAMP", isInsertedAt=True),
            Column("supplier_updated_at", "TIMESTAMP", isUpdatedAt=True),
        )
