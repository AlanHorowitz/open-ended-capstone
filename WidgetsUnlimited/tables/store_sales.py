from util.sqltypes import Table, Column
from .product import ProductTable
from .store import StoreTable


class StoreSalesTable(Table):

    NAME = "store_sales"

    def __init__(self):

        super().__init__(
            StoreSalesTable.NAME,
            Column("store_sales_id", "INTEGER", primary_key=True),
            Column(
                "store_sales_store_id",
                "INTEGER",
                xref_table=StoreTable.NAME,
                xref_column="store_id",
            ),
            Column(
                "store_sales_product_id",
                "INTEGER",
                xref_table=ProductTable.NAME,
                xref_column="product_id",
            ),
            Column("store_sales_quantity", "INTEGER"),
            Column(
                "store_sales_unit_price",
                "FLOAT",
                xref_table=ProductTable.NAME,
                xref_column="product_unit_cost",
            ),
            Column("store_sales_total_price", "FLOAT"),
            Column("store_sales_transaction_type", "VARCHAR"),
            Column("store_sales_transaction_date", "DATE"),
            Column("store_sales_cc_number", "VARCHAR", update=True),
            Column("store_sales_loyalty_number", "INTEGER"),
            Column("store_sales_inserted_at", "TIMESTAMP", inserted_at=True),
            Column("store_sales_updated_at", "TIMESTAMP", updated_at=True),
        )
