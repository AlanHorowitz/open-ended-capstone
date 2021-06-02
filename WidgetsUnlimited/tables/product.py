from util.sqltypes import Table, Column

class ProductTable(Table):

    NAME = "product"
    
    CREATE_SQL_PG = """
    CREATE TABLE IF NOT EXISTS Product (
    product_id INTEGER NOT NULL,
    product_name VARCHAR(80) NOT NULL,
    product_description VARCHAR(255) NULL DEFAULT NULL,
    product_category VARCHAR(80) NULL DEFAULT NULL,
    product_brand VARCHAR(80) NULL DEFAULT NULL,
    product_preferred_supplier_id INTEGER NULL DEFAULT NULL,
    product_unit_cost FLOAT(2) NULL DEFAULT NULL,
    product_dimension_length FLOAT(11) NULL DEFAULT NULL,
    product_dimension_width FLOAT(11) NULL DEFAULT NULL,
    product_dimension_height FLOAT(11) NULL DEFAULT NULL,
    product_introduced_date DATE NULL DEFAULT NULL,
    product_discontinued BOOLEAN NULL DEFAULT FALSE,
    product_no_longer_offered BOOLEAN NULL DEFAULT FALSE,
    product_inserted_at TIMESTAMP NOT NULL,
    product_updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (product_id));
    """

    CREATE_SQL_MYSQL = """
    CREATE TABLE IF NOT EXISTS product (
    product_id INT NOT NULL,
    product_name VARCHAR(80) NOT NULL,
    product_description VARCHAR(255) NULL DEFAULT NULL,
    product_category VARCHAR(80) NULL DEFAULT NULL,
    product_brand VARCHAR(80) NULL DEFAULT NULL,
    product_preferred_supplier_id INT NULL DEFAULT NULL,
    product_unit_cost DOUBLE NULL DEFAULT NULL,
    product_dimension_length DOUBLE NULL DEFAULT NULL,
    product_dimension_width DOUBLE NULL DEFAULT NULL,
    product_dimension_height DOUBLE NULL DEFAULT NULL,
    product_introduced_date DATE NULL DEFAULT NULL,
    product_discontinued tinyint(1) NULL DEFAULT FALSE,
    product_no_longer_offered tinyint(1) NULL DEFAULT FALSE,
    product_inserted_at TIMESTAMP(6) NOT NULL,
    product_updated_at TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (product_id));
    """

    def __init__(self):
        super().__init__(
            ProductTable.NAME,
            ProductTable.CREATE_SQL_PG,
            ProductTable.CREATE_SQL_MYSQL,
            Column("product_id", "INTEGER", isPrimaryKey=True),
            Column("product_name", "VARCHAR"),
            Column("product_description", "VARCHAR"),
            Column("product_category", "VARCHAR"),
            Column("product_brand", "VARCHAR"),
            Column("product_preferred_supplier_id", "INTEGER"),
            Column("product_unit_cost", "FLOAT"),
            Column("product_dimension_length", "FLOAT"),
            Column("product_dimension_width", "FLOAT"),
            Column("product_dimension_height", "FLOAT"),
            Column("product_introduced_date", "DATE"),
            Column("product_discontinued", "BOOLEAN"),
            Column("product_no_longer_offered", "BOOLEAN"),
            Column("product_inserted_at", "TIMESTAMP", isInsertedAt=True),
            Column("product_updated_at", "TIMESTAMP", isUpdatedAt=True),
        )
