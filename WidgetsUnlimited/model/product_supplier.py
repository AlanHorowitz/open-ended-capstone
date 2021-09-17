from .metadata import Table, Column


class ProductSupplierTable(Table):

    NAME = "product_supplier"

    def __init__(self):
        super().__init__(
            ProductSupplierTable.NAME,
            Column("product_id", "INTEGER", primary_key=True),
            Column("supplier_id", "INTEGER", primary_key=True),
            Column("product_supplier_inserted_at", "TIMESTAMP", inserted_at=True),
            Column("product_supplier_updated_at", "TIMESTAMP", updated_at=True),
            create_only=True,
            batch_id=True,
        )
