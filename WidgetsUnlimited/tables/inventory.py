from util.sqltypes import Table, Column


class InventoryTable(Table):

    NAME = "inventory"

    def __init__(self):
        super().__init__(
            InventoryTable.NAME,
            Column("inventory_event_id", "INTEGER", isPrimaryKey=True),
            Column("inventory_event_type", "VARCHAR"),  # shipment, supply, return
            Column("inventory_product_id", "INTEGER"),
            Column("inventory_supplier_id", "INTEGER"),
            Column("inventory_warehouse_id", "INTEGER"),
            Column("inventory_quantity", "INTEGER"),
            Column("inventory_ship_address", "VARCHAR"),
            Column("inventory_ship_customer_id", "INTEGER"),
            Column("inventory_inserted_at", "TIMESTAMP", isInsertedAt=True),
            Column("inventory_updated_at", "TIMESTAMP", isUpdatedAt=True),
        )
