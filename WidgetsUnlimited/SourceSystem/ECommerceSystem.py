from util.sqltypes import Table, Column
from typing import List
from .OperationalSourceSystem import OperationalSourceSystem

import psycopg2
from psycopg2.extras import DictCursor, DictRow
from psycopg2.extensions import connection, cursor

from util.etlutils import create_table


class eCommerceOperationalSystem(OperationalSourceSystem):
    def __init__(self) -> None:
        # open connection to postgres
        self.connection: connection = psycopg2.connect(
        "dbname=retaildw host=172.17.0.1 user=user1 password=user1"
        )

        self.cur: cursor = self.connection.cursor(cursor_factory=DictCursor)
        self.cur.execute("CREATE SCHEMA ECOMMERCE;")
        self.cur.execute("SET SEARCH_PATH TO ECOMMERCE;")
        self.connection.commit()
       
        pass

    def add_tables(self, tables : List[Table]) -> None:
        # create all the postgres tables
        for table in tables:
            table.setOperationalSystem(self)
            create_table(self.connection, table.get_create_sql_postgres())         

    def remove_tables():
        pass

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
        """ Perform updates with delete then insert. """
        
        n_delete_inserts = len(records)
        table_name = table.get_name()
        primary_key_column = table.get_primary_key()
        
        if n_delete_inserts > 0:

            keys = tuple([r[primary_key_column] for r in records])

            print('keys:', keys)

            self.cur.execute(
                f"DELETE FROM {table_name} WHERE {primary_key_column} in %s;",
                (keys,),
            )

            print(self.cur.rowcount, 'deleted')

            self.connection.commit()            
            insert_records = [tuple(dr.values()) for dr in records]
            self.insert(table, insert_records)

