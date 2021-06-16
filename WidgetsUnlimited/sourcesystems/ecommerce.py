from typing import List
import os

import psycopg2
from psycopg2.extras import DictCursor, DictRow
from psycopg2.extensions import connection, cursor

from .base import BaseSystem
from util.sqltypes import Table, Column
from .generator import DataGenerator

class eCommerceSystem(BaseSystem):
    def __init__(self, dataGenerator : DataGenerator) -> None:
        # open connection to postgres
        super().__init__(dataGenerator)
        self.connection: connection = psycopg2.connect(
            dbname = os.environ['E_COMMERCE_DB'],
            host = os.environ['E_COMMERCE_HOST'],
            port = os.environ['E_COMMERCE_PORT'],
            user = os.environ['E_COMMERCE_USER'],
            password = os.environ['E_COMMERCE_PASSWORD'],
        )
        
        schema = os.environ['E_COMMERCE_SCHEMA']
        self.cur: cursor = self.connection.cursor(cursor_factory=DictCursor)
        self.cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")   
        self.cur.execute(f"SET SEARCH_PATH TO {schema};")     

        self.connection.commit()
               
    def add_tables(self, tables : List[Table]) -> None:
        super().add_tables(tables)
        for table in tables:
            self.cur.execute(table.get_create_sql_postgres())
            self.connection.commit()
    
    def open(table : Table) -> None:
        pass

    def close(table):
        pass

    def insert(self, table, records):
        
        n_inserts = len(records)
        table_name = table.get_name()
        column_names = ",".join(table.get_column_names()) 
        if n_inserts > 0:
            
            values_substitutions = ",".join(
                ["%s"] * n_inserts
            )  # each %s holds one tuple row

            self.cur.execute(
                f"INSERT INTO {table_name} ({column_names}) values {values_substitutions}",
                records,
            )

            self.connection.commit()
        

    def update(self, table, records):
        """ Perform updates as delete then insert. """        
        n_delete_inserts = len(records)
        table_name = table.get_name()
        primary_key_column = table.get_primary_key()
        
        if n_delete_inserts > 0:

            keys = tuple([r[primary_key_column] for r in records])

            self.cur.execute(
                f"DELETE FROM {table_name} WHERE {primary_key_column} in %s;",
                (keys,),
            )
           
            self.connection.commit()            
            insert_records = [tuple(dr.values()) for dr in records] # DictRow to tuple
            self.insert(table, insert_records)

