from util.sqltypes import Table, Column
from .product import PRODUCT_TABLE
from .store import STORE_TABLE

STORE_SALES_CREATE_SQL_PG = """
CREATE TABLE IF NOT EXISTS store_sales (
store_sales_id INTEGER NOT NULL,
store_sales_store_id INTEGER NOT NULL,
store_sales_product_id INTEGER NOT NULL,
store_sales_quantity INTEGER DEFAULT NULL,
store_sales_unit_price FLOAT(2),
store_sales_total_price FLOAT(2),
store_sales_transaction_type VARCHAR(80),
store_sales_transaction_date DATE NULL DEFAULT NULL,
store_sales_inserted_at TIMESTAMP(6) NOT NULL,
store_sales_updated_at TIMESTAMP(6) NOT NULL,
PRIMARY KEY (store_sales_id));
"""

STORE_SALES_CREATE_SQL_MYSQL = """
CREATE TABLE IF NOT EXISTS store_sales (
store_sales_id INTEGER NOT NULL,
store_sales_store_id INTEGER NOT NULL,
store_sales_product_id INTEGER NOT NULL,
store_sales_quantity INTEGER DEFAULT NULL,
store_sales_unit_price DOUBLE,
store_sales_total_price DOUBLE,
store_sales_transaction_type VARCHAR(80),
store_sales_transaction_date DATE NULL DEFAULT NULL,
store_sales_inserted_at TIMESTAMP(6) NOT NULL,
store_sales_updated_at TIMESTAMP(6) NOT NULL,
PRIMARY KEY (store_sales_id));
"""

STORE_SALES_TABLE = Table(
    "store_sales",
    STORE_SALES_CREATE_SQL_PG,
    STORE_SALES_CREATE_SQL_MYSQL,
    Column("store_sales_id", "INTEGER", isPrimaryKey=True),
    Column(
        "store_sales_store_id",
        "INTEGER",
        xref_table=STORE_TABLE.get_name(),
        xref_column="store_id",
    ),
    Column(
        "store_sales_product_id",
        "INTEGER",
        xref_table=PRODUCT_TABLE.get_name(),
        xref_column="product_id",
    ),
    Column("store_sales_quantity", "INTEGER"),
    Column(
        "store_sales_unit_price",
        "FLOAT",
        xref_table=PRODUCT_TABLE.get_name(),
        xref_column="product_unit_cost",
    ),
    Column("store_sales_total_price", "FLOAT"),
    Column("store_sales_transaction_type", "VARCHAR"),
    Column("store_sales_transaction_date", "DATE"),
    Column("store_sales_inserted_at", "TIMESTAMP", isInsertedAt=True),
    Column("store_sales_updated_at", "TIMESTAMP", isUpdatedAt=True),
)
