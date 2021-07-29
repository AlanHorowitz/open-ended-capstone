from util.sqltypes import Table, Column

class SupplierTable(Table):

    NAME = "supplier"
    
    CREATE_SQL_PG = """
    CREATE TABLE IF NOT EXISTS supplier (
    supplier_id INTEGER NOT NULL,
    supplier_name VARCHAR(80) NOT NULL,
    supplier_description VARCHAR(255) NULL DEFAULT NULL,
    supplier_category VARCHAR(80) NULL DEFAULT NULL,
    supplier_brand VARCHAR(80) NULL DEFAULT NULL,
    supplier_preferred_supplier_id INTEGER NULL DEFAULT NULL,
    supplier_unit_cost FLOAT(2) NULL DEFAULT NULL,
    supplier_dimension_length FLOAT(11) NULL DEFAULT NULL,
    supplier_dimension_width FLOAT(11) NULL DEFAULT NULL,
    supplier_dimension_height FLOAT(11) NULL DEFAULT NULL,
    supplier_introduced_date DATE NULL DEFAULT NULL,
    supplier_discontinued BOOLEAN NULL DEFAULT FALSE,
    supplier_no_longer_offered BOOLEAN NULL DEFAULT FALSE,
    supplier_inserted_at TIMESTAMP NOT NULL,
    supplier_updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (supplier_id));
    """

    CREATE_SQL_MYSQL = """
    CREATE TABLE IF NOT EXISTS supplier (
    supplier_id INT NOT NULL,
    supplier_name VARCHAR(80) NOT NULL,
    supplier_description VARCHAR(255) NULL DEFAULT NULL,
    supplier_address VARCHAR(80) NULL DEFAULT NULL,
    supplier_brand VARCHAR(80) NULL DEFAULT NULL,
    supplier_preferred_supplier_id INT NULL DEFAULT NULL,
    supplier_unit_cost DOUBLE NULL DEFAULT NULL,
    supplier_dimension_length DOUBLE NULL DEFAULT NULL,
    supplier_dimension_width DOUBLE NULL DEFAULT NULL,
    supplier_dimension_height DOUBLE NULL DEFAULT NULL,
    supplier_introduced_date DATE NULL DEFAULT NULL,
    supplier_discontinued tinyint(1) NULL DEFAULT FALSE,
    supplier_no_longer_offered tinyint(1) NULL DEFAULT FALSE,
    supplier_inserted_at TIMESTAMP(6) NOT NULL,
    supplier_updated_at TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (supplier_id));
    """

    def __init__(self):
        super().__init__(
            SupplierTable.NAME,
            SupplierTable.CREATE_SQL_PG,
            SupplierTable.CREATE_SQL_MYSQL,
            Column("supplier_id", "INTEGER", isPrimaryKey=True),
            Column("supplier_name", "VARCHAR"),
            Column("supplier_address", "VARCHAR"),
            Column("supplier_primary_contact_name", "VARCHAR"),
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
