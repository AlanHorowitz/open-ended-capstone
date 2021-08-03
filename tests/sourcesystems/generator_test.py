import sys
import os
import pytest

print("generator_test",sys.path)

from .context import DataGenerator
from .context import TableUpdate
from .context import OrderTable
from .context import OrderLineItemTable

print("generator_test",sys.path)

@pytest.fixture
def data_generator():
    
    # test docker image
    os.environ['DATA_GENERATOR_DB'] = 'postgres'
    os.environ['DATA_GENERATOR_HOST'] = '172.17.0.2'
    os.environ['DATA_GENERATOR_PORT'] = '5432'
    os.environ['DATA_GENERATOR_USER'] = 'postgres'
    os.environ['DATA_GENERATOR_PASSWORD'] = 'postgres'
    os.environ['DATA_GENERATOR_SCHEMA'] = 'test'

    generator = DataGenerator()

    yield generator
    # order_table = OrderTable()
    # order_line_item_table = OrderLineItemTable()

    # generator.add_tables([order_table, order_line_item_table])

    # generator.generate(TableUpdate(order_table, n_inserts=10, n_updates=0, batch_id=1))
    # generator.generate(TableUpdate(order_line_item_table, n_inserts=3,
    #                             n_updates=0, batch_id=1, link_parent=True))

@pytest.fixture
def order_table(data_generator):
    o : OrderTable = OrderTable()        
    cursor = data_generator.cur
    cursor.execute(o.get_create_sql_postgres())
    yield o

def test_generate_insert(data_generator, order_table):    
    
    cursor = data_generator.cur
    data_generator.generate(TableUpdate(order_table, n_inserts=10, n_updates=0, batch_id=1))

    cursor.execute("select * from order1")
    assert len(cursor.fetchall()) == 10

def test_generate_update():
    pass

def test_generate_insert_and_update():
    pass

def test_generate_link_parent(data_generator):
    print("generator_test",sys.path)

    generator = data_generator
    pass

def test_generate_link_parent_invalid():
    pass

def test_generate_single_xref():
    pass

def test_generate_multiple_xref():
    pass


