import os
# from _pytest.recwarn import T
import pytest
from typing import Dict
from datetime import datetime

from .context import Table, Column, DEFAULT_INSERT_VALUES
from .context import DataGenerator
from .context import TableUpdate
from .context import OrderTable
from .context import ProductTable
from .context import CustomerTable
from .context import OrderLineItemTable

def create_and_return_table(cursor, table):
    cursor.execute(f"DROP TABLE IF EXISTS {table.get_name()};")
    cursor.execute(table.get_create_sql_postgres())
    return table

@pytest.fixture
def data_generator():
    
    # test docker image
    # os.environ['DATA_GENERATOR_DB'] = 'postgres'
    os.environ['DATA_GENERATOR_DB'] = 'retaildw'
    # os.environ['DATA_GENERATOR_HOST'] = '172.17.0.2'
    os.environ['DATA_GENERATOR_HOST'] = '172.18.0.1'
    os.environ['DATA_GENERATOR_PORT'] = '5432'
    # os.environ['DATA_GENERATOR_USER'] = 'postgres'
    # os.environ['DATA_GENERATOR_PASSWORD'] = 'postgres'
    os.environ['DATA_GENERATOR_USER'] = 'user1'
    os.environ['DATA_GENERATOR_PASSWORD'] = 'user1'
    os.environ['DATA_GENERATOR_SCHEMA'] = 'test'

    generator = DataGenerator()

    yield generator   

@pytest.fixture
def order_table(data_generator):
    print("In order table")
    yield create_and_return_table(data_generator.cur, OrderTable())

@pytest.fixture
def order_line_item_table(data_generator):    
    yield create_and_return_table(data_generator.cur, OrderLineItemTable())    
    
@pytest.fixture
def product_table(data_generator):    
    yield create_and_return_table(data_generator.cur, ProductTable())

@pytest.fixture
def customer_table(data_generator):    
    yield create_and_return_table(data_generator.cur, CustomerTable())

def make_rows(cursor, table : Table, n_rows :int=10, 
              start_key :int = 1, batch_id :int = 0):

    insert_rows = []
    key = start_key
    table_name = table.get_name()    
    column_names = ",".join(table.get_column_names())
    values_substitutions = ",".join(["%s"] * n_rows)
    for _ in range(n_rows):
        d = []
        for col in table.get_columns():
            if col.isPrimaryKey():
                d.append(key)    
            elif col.isBatchId():
                d.append(batch_id) 
            else:
                d.append(DEFAULT_INSERT_VALUES[col.get_type()])        
        key += 1
        insert_rows.append(tuple(d))

    cursor.execute(f"INSERT INTO {table_name} ({column_names}) "
                       f" values {values_substitutions}", insert_rows)

def test_generate_insert(data_generator, customer_table):    
    
    data_generator.generate(TableUpdate(customer_table, n_inserts=10, n_updates=0, batch_id=1))
    cursor = data_generator.cur
    cursor.execute(f"select * from {customer_table.get_name()}")
    assert len(cursor.fetchall()) == 10

def test_generate_update(data_generator, customer_table):

    cursor = data_generator.cur
    make_rows(cursor, customer_table, n_rows = 10, start_key = 1)
    data_generator.generate(TableUpdate(customer_table, n_inserts=0, n_updates=5, batch_id=1))    
    cursor.execute(f"select * from {customer_table.get_name()}")
    assert len(cursor.fetchall()) == 10
    cursor.execute(f"select * from {customer_table.get_name()} "
                   f"WHERE customer_email LIKE '%_UPD'")
    assert len(cursor.fetchall()) == 5    

def test_generate_insert_and_update(data_generator, customer_table):

    cursor = data_generator.cur
    make_rows(cursor, customer_table, n_rows = 10, start_key = 1)
    data_generator.generate(TableUpdate(customer_table, n_inserts=15, n_updates=5, batch_id=1))    
    cursor.execute(f"select * from {customer_table.get_name()}")
    assert len(cursor.fetchall()) == 25
    cursor.execute(f"select * from {customer_table.get_name()} "
                   f"WHERE customer_email LIKE '%_UPD'")
    assert len(cursor.fetchall()) == 5 

def test_generate_link_parent(data_generator, order_table, order_line_item_table, product_table):
    cursor = data_generator.cur

    make_rows(cursor, product_table, n_rows = 5, start_key = 1, batch_id = 1)
    make_rows(cursor, order_table, n_rows = 10, start_key = 1, batch_id = 1)
    data_generator.generate(TableUpdate(order_line_item_table,
                   n_inserts=3, n_updates=0, batch_id=1, link_parent=True))
    cursor.execute(f"select * from {order_table.get_name()}")
    assert len(cursor.fetchall()) == 10
    cursor.execute(f"select * from {order_line_item_table.get_name()}")
    assert len(cursor.fetchall()) == 30
    cursor.execute(f"select * from {order_line_item_table.get_name()}"
    " WHERE order_id = 4;")
    assert len(cursor.fetchall()) == 3

    # test with second batch and new values
    make_rows(cursor, order_table, n_rows = 20, start_key = 40, batch_id = 2)
    data_generator.generate(TableUpdate(order_line_item_table,
                   n_inserts=5, n_updates=0, batch_id=2, link_parent=True))
    cursor.execute(f"select * from {order_table.get_name()}")
    assert len(cursor.fetchall()) == 30 # 10 + 20
    cursor.execute(f"select * from {order_line_item_table.get_name()}")
    assert len(cursor.fetchall()) == 130 # 30 + 20 * 5
    cursor.execute(f"select * from {order_line_item_table.get_name()}"
    " WHERE order_id = 7;")
    assert len(cursor.fetchall()) == 3 # id from batch 1    
    cursor.execute(f"select * from {order_line_item_table.get_name()}"
    " WHERE order_id = 42;")
    assert len(cursor.fetchall()) == 5 # id from batch 2


def test_generate_link_parent_invalid(data_generator, product_table):
    with pytest.raises(Exception):        
        data_generator.generate(TableUpdate(product_table,
                   n_inserts=5, n_updates=0, batch_id=1, link_parent=True))
    cursor = data_generator.cur
    cursor.execute(f"select * from {product_table.get_name()}")
    assert len(cursor.fetchall()) == 0 

def test_set_default(data_generator, customer_table):
    data_generator.generate(TableUpdate(customer_table, n_inserts=1, n_updates=0, batch_id=1))
    cursor = data_generator.cur
    cursor.execute(f"select customer_referral_type, customer_sex from {customer_table.get_name()}")
    rs = cursor.fetchone()
    assert rs == ['OA', 'F']

def test_generate_multiple_xref(data_generator, order_table, order_line_item_table, product_table):
    cursor = data_generator.cur
    make_rows(cursor, product_table, n_rows = 5, start_key = 1, batch_id = 1)
    make_rows(cursor, order_table, n_rows = 10, start_key = 1, batch_id = 1)
    cursor.execute(f"update {product_table.get_name()} set product_unit_cost = product_id;")
    data_generator.generate(TableUpdate(order_line_item_table,
                   n_inserts=3, n_updates=0, batch_id=1, link_parent=True))

    count_correct_xref = f" SELECT count(order_line_item_id) from {order_line_item_table.get_name()}"\
    f" WHERE order_line_item_product_id = order_line_item_unit_price; "
    cursor.execute(count_correct_xref)
    rs = cursor.fetchone()
    assert rs[0] == 30 


