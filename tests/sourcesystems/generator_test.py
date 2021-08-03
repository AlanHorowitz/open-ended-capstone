import sys
import os
from _pytest.recwarn import T
import pytest
from typing import Dict
from datetime import datetime

from .context import Table, Column, DEFAULT_INSERT_VALUES
from .context import DataGenerator
from .context import TableUpdate
from .context import OrderTable
from .context import OrderLineItemTable

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

@pytest.fixture
def order_table(data_generator):
    table : OrderTable = OrderTable()        
    cursor = data_generator.cur
    cursor.execute(f"DROP TABLE IF EXISTS {table.get_name()};")
    cursor.execute(table.get_create_sql_postgres())
    yield table

def make_rows(cursor, table : Table, n_rows :int=10, start_key :int = 1):

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
            else:
                d.append(DEFAULT_INSERT_VALUES[col.get_type()])        
        key += 1
        insert_rows.append(tuple(d))

    cursor.execute(f"INSERT INTO {table_name} ({column_names}) "
                       f" values {values_substitutions}", insert_rows)

def test_generate_insert(data_generator, order_table):    
    
    data_generator.generate(TableUpdate(order_table, n_inserts=10, n_updates=0, batch_id=1))
    cursor = data_generator.cur
    cursor.execute(f"select * from {order_table.get_name()}")
    assert len(cursor.fetchall()) == 10

def test_generate_update(data_generator, order_table):

    cursor = data_generator.cur
    make_rows(cursor, order_table, n_rows = 10, start_key = 1)
    data_generator.generate(TableUpdate(order_table, n_inserts=0, n_updates=5, batch_id=1))    
    cursor.execute(f"select * from {order_table.get_name()}")
    assert len(cursor.fetchall()) == 10
    cursor.execute(f"select * from {order_table.get_name()} "
                   f"WHERE order_special_instructions LIKE '%_UPD'")
    assert len(cursor.fetchall()) == 5    

def test_generate_insert_and_update(data_generator, order_table):

    cursor = data_generator.cur
    make_rows(cursor, order_table, n_rows = 10, start_key = 1)
    data_generator.generate(TableUpdate(order_table, n_inserts=15, n_updates=5, batch_id=1))    
    cursor.execute(f"select * from {order_table.get_name()}")
    assert len(cursor.fetchall()) == 25
    cursor.execute(f"select * from {order_table.get_name()} "
                   f"WHERE order_special_instructions LIKE '%_UPD'")
    assert len(cursor.fetchall()) == 5 

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


