# Integration test -- test data create_only and warehouse
import os

from mysql.connector import connect

from .context import CustomerDimensionProcessor
from .context import DataGenerator, GeneratorRequest
from .context import extract_write_stage

from .context import ProductTable
from .context import ProductSupplierTable
from .context import CustomerTable
from .context import CustomerAddressTable


def test_generator_and_transform():

    ms_connection = connect(
        host=os.getenv("WAREHOUSE_HOST"),
        port=os.getenv("WAREHOUSE_PORT"),
        user=os.getenv("WAREHOUSE_USER"),
        password=os.getenv("WAREHOUSE_PASSWORD"),
        database=os.getenv("WAREHOUSE_DB"),
        charset="utf8",
    )
    data_generator = DataGenerator()
    product_table = ProductTable()
    product_suppliers_table = ProductSupplierTable()
    customer_table = CustomerTable()
    customer_address_table = CustomerAddressTable()
    customer_dimension = CustomerDimensionProcessor(ms_connection)

    data_generator.add_tables([product_table, product_suppliers_table, customer_table, customer_address_table])

    data_generator.generate(GeneratorRequest(product_table, n_inserts=10))

    # 20 customers with corresponding address
    data_generator.generate(GeneratorRequest(customer_table, n_inserts=20), 1)
    data_generator.generate(
        GeneratorRequest(customer_address_table, n_inserts=1, link_parent=True), 1
    )

    extract_write_stage(
        data_generator.get_connection(),
        batch_id=1,
        tables=[customer_table, customer_address_table],
    )
    customer_dimension.process_update(1)

    cur = ms_connection.cursor()
    cur.execute("SELECT COUNT(*) from customer_dim;")

    assert cur.fetchone()[0] == 20

    # try modifying two customers
    data_generator.generate(
        GeneratorRequest(customer_table, n_inserts=0, n_updates=2), batch_id=2
    )

    extract_write_stage(
        data_generator.get_connection(),
        batch_id=2,
        tables=[customer_table, customer_address_table],
    )
    customer_dimension.process_update(2)

    cur.execute("SELECT COUNT(*) from customer_dim;")
    assert cur.fetchone()[0] == 20

    cur.execute("SELECT COUNT(email) from customer_dim where email like '%_UPD';")
    assert cur.fetchone()[0] == 2
