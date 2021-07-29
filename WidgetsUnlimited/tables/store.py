from util.sqltypes import Column, Table

class StoreTable(Table):

    NAME = "store"

    CREATE_SQL_PG = """
    CREATE TABLE IF NOT EXISTS store (
    store_id INTEGER NOT NULL,
    store_name VARCHAR(80) NOT NULL,
    store_manager_name VARCHAR(255) NULL DEFAULT NULL,
    store_number_of_employees INTEGER NULL DEFAULT NULL,
    store_opened_date DATE NULL DEFAULT NULL,
    store_closed_date DATE NULL DEFAULT NULL,
    store_inserted_at TIMESTAMP(6) NOT NULL,
    store_updated_at TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (store_id));
    """

    CREATE_SQL_MYSQL = """
    CREATE TABLE IF NOT EXISTS store (
    store_id INT NOT NULL,
    store_name VARCHAR(80) NOT NULL,
    store_manager_name VARCHAR(255) NULL DEFAULT NULL,
    store_number_of_employees INT NULL DEFAULT NULL,
    store_opened_date DATE NULL DEFAULT NULL,
    store_closed_date DATE NULL DEFAULT NULL,
    store_inserted_at TIMESTAMP(6) NOT NULL,
    store_updated_at TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (store_id));
    """

    def __init__(self):
        super().__init__(
            StoreTable.NAME,
            Column("store_id", "INTEGER", isPrimaryKey=True),
            Column("store_name", "VARCHAR"),
            Column("store_manager_name", "VARCHAR"),
            Column("store_number_of_employees", "INTEGER"),
            Column("store_opened_date", "DATE"),
            Column("store_closed_date", "DATE"),
            Column("store_inserted_at", "TIMESTAMP", isInsertedAt=True),
            Column("store_updated_at", "TIMESTAMP", isUpdatedAt=True),
        )
